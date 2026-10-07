"""协调端：仅保存元数据（SQLite），不落地任何可恢复的整件内容。

职责：
  - 接收客户端 PUT（原文 + 请求键 + 期望版本），拆片、算摘要，把三片分别写入三台节点；
  - 至少两片确认持久化且期望版本仍是当前版，才发布递增版本（CAS）；
  - GET 固定请求开始时的已发布版本，从任意两片可信分片还原并校验整件摘要；
  - 同名同键重试幂等：已发布返回原版本，未发布续交（先查询节点已有片）；
  - 修复：按当前版本补齐缺损片，版本变化即放弃，绝不用旧片覆盖新版。
"""
from __future__ import annotations

import asyncio
import os
import sqlite3
import threading
import time
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response

from .common import (
    MAX_OBJECT_SIZE,
    MIN_OBJECT_SIZE,
    REQUEST_KEY_MAX_LEN,
    derive_shard,
    reconstruct,
    sha256_hex,
    split_shards,
    valid_name,
)

DB_PATH = os.environ.get("DB_PATH", "./data/coordinator.db")
NODES = [
    n.strip().rstrip("/")
    for n in os.environ.get(
        "NODES", "http://localhost:8001,http://localhost:8002,http://localhost:8003"
    ).split(",")
    if n.strip()
]
# 节点 i 固定存放第 i 片
MIN_QUORUM = 2

SCHEMA = """
CREATE TABLE IF NOT EXISTS objects (
  name            TEXT PRIMARY KEY,
  current_version INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS versions (
  name             TEXT NOT NULL,
  version          INTEGER NOT NULL,
  request_key      TEXT NOT NULL,
  expected_version INTEGER NOT NULL,
  size             INTEGER NOT NULL,
  sha256           TEXT NOT NULL,
  shard_len        INTEGER NOT NULL,
  shard_sha0       TEXT NOT NULL,
  shard_sha1       TEXT NOT NULL,
  shard_sha2       TEXT NOT NULL,
  state            TEXT NOT NULL CHECK (state IN ('pending', 'published')),
  created_at       REAL NOT NULL,
  published_at     REAL,
  PRIMARY KEY (name, version),
  UNIQUE (name, request_key)
);
CREATE TABLE IF NOT EXISTS shard_copies (
  name        TEXT NOT NULL,
  version     INTEGER NOT NULL,
  shard_index INTEGER NOT NULL,
  node        TEXT NOT NULL,
  sha256      TEXT NOT NULL,
  stored_at   REAL NOT NULL,
  PRIMARY KEY (name, version, shard_index, node)
);
"""


class MetadataDB:
    """协调端元数据库。只存摘要与状态，绝不存整件或分片内容。"""

    def __init__(self, path: str):
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.executescript(SCHEMA)
        self._conn.commit()
        self._lock = threading.RLock()

    def current_version(self, name: str) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT current_version FROM objects WHERE name = ?", (name,)
            ).fetchone()
            return int(row["current_version"]) if row else 0

    def next_version(self, name: str) -> int:
        """下一个可分配的版本号：取所有版本（含未发布 pending）的最大值 +1。

        并发提交可能各自持有 pending 版本，已发布版本号只在发布时推进，
        因此分配必须基于 versions 表的最大版本，避免两个 pending 撞号。
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT COALESCE(MAX(version), 0) AS mv FROM versions WHERE name = ?",
                (name,),
            ).fetchone()
            return int(row["mv"]) + 1

    def version_by_key(self, name: str, key: str):
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM versions WHERE name = ? AND request_key = ?",
                (name, key),
            ).fetchone()

    def get_version(self, name: str, version: int):
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM versions WHERE name = ? AND version = ?",
                (name, version),
            ).fetchone()

    def published_version(self, name: str):
        """当前已发布版本的完整元数据；对象不存在或未发布时返回 None。"""
        with self._lock:
            return self._conn.execute(
                """
                SELECT v.* FROM objects o
                JOIN versions v ON v.name = o.name AND v.version = o.current_version
                WHERE o.name = ? AND o.current_version > 0
                """,
                (name,),
            ).fetchone()

    def insert_pending(
        self,
        name: str,
        version: int,
        key: str,
        expected: int,
        size: int,
        digest: str,
        shard_len: int,
        shard_shas: list[str],
    ) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR IGNORE INTO objects(name, current_version) VALUES (?, 0)",
                (name,),
            )
            self._conn.execute(
                """
                INSERT INTO versions(
                  name, version, request_key, expected_version, size, sha256,
                  shard_len, shard_sha0, shard_sha1, shard_sha2, state, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
                """,
                (
                    name,
                    version,
                    key,
                    expected,
                    size,
                    digest,
                    shard_len,
                    shard_shas[0],
                    shard_shas[1],
                    shard_shas[2],
                    time.time(),
                ),
            )
            self._conn.commit()

    def record_copy(self, name: str, version: int, idx: int, node: str, digest: str) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO shard_copies(name, version, shard_index, node, sha256, stored_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (name, version, idx, node, digest, time.time()),
            )
            self._conn.commit()

    def mark_published(self, name: str, version: int) -> None:
        """发布版本：对象当前版本单调递增。调用方须已校验 expected == current。"""
        with self._lock:
            self._conn.execute(
                "UPDATE versions SET state = 'published', published_at = ? "
                "WHERE name = ? AND version = ?",
                (time.time(), name, version),
            )
            self._conn.execute(
                "UPDATE objects SET current_version = ? WHERE name = ?",
                (version, name),
            )
            self._conn.commit()

    def copies(self, name: str, version: int):
        with self._lock:
            return self._conn.execute(
                "SELECT shard_index, node, sha256, stored_at FROM shard_copies "
                "WHERE name = ? AND version = ? ORDER BY shard_index, node",
                (name, version),
            ).fetchall()


# ---------------------------------------------------------------------------
# 应用状态
# ---------------------------------------------------------------------------

db: MetadataDB
client: httpx.AsyncClient
# 串行化“分配版本号 / 发布”这两个临界区；网络 I/O 不持锁
state_lock = asyncio.Lock()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global db, client
    db = MetadataDB(DB_PATH)
    client = httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=3.0))
    yield
    await client.aclose()


app = FastAPI(title="shard-coordinator", lifespan=lifespan)


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------


def _shard_shas(row) -> list[str]:
    return [row["shard_sha0"], row["shard_sha1"], row["shard_sha2"]]


def _version_payload(row, replayed: bool) -> dict:
    return {
        "name": row["name"],
        "version": row["version"],
        "size": row["size"],
        "sha256": row["sha256"],
        "state": row["state"],
        "replayed": replayed,
    }


async def _ensure_shards(name: str, version: int, shards: list[bytes], shard_shas: list[str]) -> int:
    """把三片写入对应节点，返回确认持久化的片数。

    续交语义：先 HEAD 查询节点已有片（节点存片后丢失回执、协调端重启的场景），
    摘要一致即视为已确认；否则再 PUT。节点按写入标识不可变存片，重复 PUT 幂等。
    """

    async def one(idx: int, node: str) -> bool:
        try:
            r = await client.head(f"{node}/shards/{name}/{version}/{idx}")
            if r.status_code == 200 and r.headers.get("x-actual-sha256") == shard_shas[idx]:
                db.record_copy(name, version, idx, node, shard_shas[idx])
                return True
        except httpx.HTTPError:
            pass
        try:
            r = await client.put(
                f"{node}/shards/{name}/{version}/{idx}",
                content=shards[idx],
                headers={"X-Shard-Sha256": shard_shas[idx]},
            )
            if r.status_code in (200, 201):
                db.record_copy(name, version, idx, node, shard_shas[idx])
                return True
        except httpx.HTTPError:
            pass
        return False

    results = await asyncio.gather(*(one(i, n) for i, n in enumerate(NODES)))
    return sum(1 for ok in results if ok)


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------


@app.get("/healthz")
def healthz():
    return {"ok": True, "role": "coordinator", "nodes": NODES}


@app.put("/objects/{name}")
async def put_object(name: str, request: Request):
    # ---- 输入校验（422）----
    if not valid_name(name):
        raise HTTPException(422, "invalid object name")
    key = request.headers.get("x-request-key", "")
    if not key or len(key) > REQUEST_KEY_MAX_LEN:
        raise HTTPException(422, "missing or invalid X-Request-Key header")
    try:
        expected = int(request.headers.get("x-expected-version", ""))
        if expected < 0:
            raise ValueError
    except ValueError:
        raise HTTPException(422, "missing or invalid X-Expected-Version header")
    body = await request.body()
    if not (MIN_OBJECT_SIZE <= len(body) <= MAX_OBJECT_SIZE):
        raise HTTPException(422, f"object size must be {MIN_OBJECT_SIZE}..{MAX_OBJECT_SIZE} bytes")
    digest = sha256_hex(body)

    # ---- 幂等登记 / 续交定位（临界区）----
    async with state_lock:
        row = db.version_by_key(name, key)
        if row is not None and row["sha256"] != digest:
            raise HTTPException(409, "request key already used with different content")
        if row is not None and row["state"] == "published":
            # 已发布请求同名同键同内容重试：返回原版本
            return JSONResponse(_version_payload(row, replayed=True), status_code=200)
        if row is None:
            current = db.current_version(name)
            if expected != current:
                raise HTTPException(
                    409, f"expected version {expected} but current version is {current}"
                )
            shards = split_shards(body)
            version = db.next_version(name)
            db.insert_pending(
                name, version, key, expected, len(body), digest,
                len(shards[0]), [sha256_hex(s) for s in shards],
            )
            row = db.get_version(name, version)
        elif row["expected_version"] != db.current_version(name):
            # 未发布请求同键续交，但期望版本已过期，直接失败（版本号只增不减，无再试意义）
            raise HTTPException(
                409,
                f"pending version {row['version']} expected {row['expected_version']} "
                f"but current version is {db.current_version(name)}",
            )

    version = row["version"]
    shard_shas = _shard_shas(row)
    shards = split_shards(body)
    # 重试内容与登记时摘要不一致已在上面拦截；这里再自检一次分片摘要
    assert [sha256_hex(s) for s in shards] == shard_shas

    # ---- 写片（网络 I/O，不持锁）----
    confirmed = await _ensure_shards(name, version, shards, shard_shas)
    if confirmed < MIN_QUORUM:
        # 不足两片：不发布，已发布版本保持不变
        raise HTTPException(
            503, f"only {confirmed}/{len(NODES)} shards persisted; version {version} not published"
        )

    # ---- 发布（临界区，CAS：期望版本仍是当前版才发布）----
    async with state_lock:
        row = db.get_version(name, version)
        if row["state"] == "published":
            # 并发同键重试：另一方已发布，返回同一版本
            return JSONResponse(_version_payload(row, replayed=True), status_code=200)
        current = db.current_version(name)
        if row["expected_version"] != current:
            raise HTTPException(
                409,
                f"expected version {row['expected_version']} no longer current "
                f"({current}); version {version} not published",
            )
        db.mark_published(name, version)
        row = db.get_version(name, version)
        return JSONResponse(_version_payload(row, replayed=False), status_code=201)


@app.get("/objects/{name}")
async def get_object(name: str):
    if not valid_name(name):
        raise HTTPException(422, "invalid object name")
    # 固定请求开始时的已发布版本；之后的并发覆盖不影响本次读取
    row = db.published_version(name)
    if row is None:
        raise HTTPException(404, "object not found")
    version = row["version"]
    shard_shas = _shard_shas(row)

    async def fetch(idx: int, node: str):
        try:
            r = await client.get(f"{node}/shards/{name}/{version}/{idx}")
            # 只信任与已发布摘要一致的片；版本化不可变路径保证不会混片
            if r.status_code == 200 and sha256_hex(r.content) == shard_shas[idx]:
                return r.content
        except httpx.HTTPError:
            pass
        return None

    results = await asyncio.gather(*(fetch(i, n) for i, n in enumerate(NODES)))
    good = {i: data for i, data in enumerate(results) if data is not None}
    if len(good) < MIN_QUORUM:
        raise HTTPException(503, f"only {len(good)}/{len(NODES)} trustworthy shards available")
    data = reconstruct(good, row["size"])
    if sha256_hex(data) != row["sha256"]:
        raise HTTPException(503, "reconstructed object failed whole-digest verification")
    return Response(
        content=data,
        media_type="application/octet-stream",
        headers={
            "X-Object-Version": str(version),
            "X-Object-Sha256": row["sha256"],
            "X-Object-Size": str(row["size"]),
        },
    )


@app.get("/objects/{name}/meta")
def get_meta(name: str):
    if not valid_name(name):
        raise HTTPException(422, "invalid object name")
    row = db.published_version(name)
    if row is None:
        raise HTTPException(404, "object not found")
    copies = db.copies(name, row["version"])
    return {
        "name": row["name"],
        "version": row["version"],
        "size": row["size"],
        "sha256": row["sha256"],
        "shard_len": row["shard_len"],
        "shard_sha256": _shard_shas(row),
        "copies": [
            {"shard_index": c["shard_index"], "node": c["node"], "sha256": c["sha256"]}
            for c in copies
        ],
    }


@app.post("/objects/{name}/repair")
async def repair_object(name: str):
    """按当前已发布版本补齐缺损片。

    与覆盖并发时：写每一片前重新确认版本未变；版本已推进则放弃（409），
    且分片按 (name, version) 不可变寻址，旧片永远落不到新版本上。
    """
    if not valid_name(name):
        raise HTTPException(422, "invalid object name")
    row = db.published_version(name)  # 固定修复开始时的当前版本
    if row is None:
        raise HTTPException(404, "object not found")
    version = row["version"]
    shard_shas = _shard_shas(row)

    async def probe(idx: int, node: str) -> str:
        try:
            r = await client.head(f"{node}/shards/{name}/{version}/{idx}")
            if r.status_code == 200:
                return "ok" if r.headers.get("x-actual-sha256") == shard_shas[idx] else "corrupt"
            if r.status_code == 404:
                return "missing"
            return "unreachable"
        except httpx.HTTPError:
            return "unreachable"

    statuses = await asyncio.gather(*(probe(i, n) for i, n in enumerate(NODES)))
    good_idx = [i for i, s in enumerate(statuses) if s == "ok"]
    if len(good_idx) < MIN_QUORUM:
        raise HTTPException(503, "fewer than 2 intact shards; cannot repair")

    # 取回完好片（已按已发布摘要校验），用于推导缺损片
    have: dict[int, bytes] = {}

    async def fetch(idx: int):
        try:
            r = await client.get(f"{NODES[idx]}/shards/{name}/{version}/{idx}")
            if r.status_code == 200 and sha256_hex(r.content) == shard_shas[idx]:
                have[idx] = r.content
        except httpx.HTTPError:
            pass

    await asyncio.gather(*(fetch(i) for i in good_idx))
    if len(have) < MIN_QUORUM:
        raise HTTPException(503, "fewer than 2 intact shards; cannot repair")

    repaired, failed = [], []
    for idx, status in enumerate(statuses):
        if status == "ok":
            continue
        if db.current_version(name) != version:
            raise HTTPException(
                409, f"object advanced past version {version} during repair; aborting"
            )
        shard = derive_shard(have, idx)
        # 输入片均已校验摘要，推导出的片必然匹配已发布摘要
        assert sha256_hex(shard) == shard_shas[idx]
        try:
            r = await client.put(
                f"{NODES[idx]}/shards/{name}/{version}/{idx}",
                content=shard,
                headers={"X-Shard-Sha256": shard_shas[idx]},
            )
            if r.status_code in (200, 201):
                db.record_copy(name, version, idx, NODES[idx], shard_shas[idx])
                repaired.append({"shard_index": idx, "node": NODES[idx], "was": status})
            else:
                failed.append(
                    {"shard_index": idx, "node": NODES[idx], "was": status, "code": r.status_code}
                )
        except httpx.HTTPError as exc:
            failed.append(
                {"shard_index": idx, "node": NODES[idx], "was": status, "error": str(exc)}
            )
    return {
        "name": name,
        "version": version,
        "nodes": [{"node": n, "status": s} for n, s in zip(NODES, statuses)],
        "repaired": repaired,
        "failed": failed,
    }
