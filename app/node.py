"""分片节点：在独立持久卷上按写入标识不可变地存片。

写入标识 = (对象名, 版本, 片序号) + 首次记录的分片摘要：
  - 同标识同摘要重复写入：幂等重写（可借此修复落盘后损坏的字节）；
  - 同标识异摘要：409，永不覆盖；
  - 损坏片只能凭协调端保存的已发布摘要修复。

演练开关：开启后，首次成功存片之后的所有分片读写一律返回 503（模拟节点离线）。
"""
from __future__ import annotations

import os
import threading

from fastapi import FastAPI, HTTPException, Request, Response

from .common import SHA256_HEX_LEN, sha256_hex, valid_name

DATA_DIR = os.environ.get("DATA_DIR", "./data/node")
NODE_ID = os.environ.get("NODE_ID", "node")

app = FastAPI(title="shard-node")

# ---- 演练开关状态（内存态；可用环境变量预置）----
_drill_lock = threading.Lock()
_drill = {
    "fail_after_first_store": os.environ.get("DRILL_FAIL_AFTER_FIRST_STORE", "").lower()
    in ("1", "true", "yes"),
    "stored_once": False,
}

# 串行化同节点的“检查-写入”序列，保证不可变语义无竞态
_io_lock = threading.Lock()


def _paths(name: str, version: int, idx: int) -> tuple[str, str]:
    base = os.path.join(DATA_DIR, "shards", name, str(version))
    return os.path.join(base, f"{idx}.shard"), os.path.join(base, f"{idx}.sha256")


def _check_drill() -> None:
    with _drill_lock:
        active = _drill["fail_after_first_store"] and _drill["stored_once"]
    if active:
        raise HTTPException(503, "drill mode: node fails after first store")


def _mark_stored() -> None:
    with _drill_lock:
        _drill["stored_once"] = True


def _validate(name: str, version: int, idx: int) -> None:
    if not valid_name(name):
        raise HTTPException(422, "invalid object name")
    if version < 1:
        raise HTTPException(422, "invalid version")
    if idx not in (0, 1, 2):
        raise HTTPException(422, "invalid shard index")


def _atomic_write(path: str, data: bytes) -> None:
    """tmp 文件 + fsync + rename + 目录 fsync，确保持久化后才算确认。"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp-{os.getpid()}-{threading.get_ident()}"
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    dfd = os.open(os.path.dirname(path), os.O_RDONLY)
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)


def _read_sidecar(path: str):
    try:
        with open(path, "r", encoding="ascii") as f:
            return f.read().strip()
    except OSError:
        return None


@app.get("/healthz")
def healthz():
    with _drill_lock:
        drill = dict(_drill)
    return {"ok": True, "role": "node", "node": NODE_ID, "drill": drill}


@app.put("/shards/{name}/{version}/{idx}")
async def put_shard(name: str, version: int, idx: int, request: Request):
    _check_drill()
    _validate(name, version, idx)
    digest = request.headers.get("x-shard-sha256", "")
    if len(digest) != SHA256_HEX_LEN:
        raise HTTPException(422, "missing or invalid X-Shard-Sha256 header")
    body = await request.body()
    if sha256_hex(body) != digest:
        raise HTTPException(422, "shard bytes do not match X-Shard-Sha256")

    shard_path, sidecar_path = _paths(name, version, idx)
    with _io_lock:
        if os.path.exists(shard_path):
            recorded = _read_sidecar(sidecar_path)
            if recorded is None:
                # 侧车丢失（崩溃窗口）：以现存字节实际摘要作为已记录身份
                with open(shard_path, "rb") as f:
                    recorded = sha256_hex(f.read())
            if recorded != digest:
                raise HTTPException(409, "shard exists with different digest (immutable)")
            # 同一写入标识：重写以修复落盘后损坏的字节
            _atomic_write(shard_path, body)
            _atomic_write(sidecar_path, digest.encode("ascii"))
            _mark_stored()
            return Response(status_code=200)
        _atomic_write(shard_path, body)
        _atomic_write(sidecar_path, digest.encode("ascii"))
        _mark_stored()
        return Response(status_code=201)


@app.get("/shards/{name}/{version}/{idx}")
def get_shard(name: str, version: int, idx: int):
    _check_drill()
    _validate(name, version, idx)
    shard_path, sidecar_path = _paths(name, version, idx)
    if not os.path.exists(shard_path):
        raise HTTPException(404, "shard not found")
    with open(shard_path, "rb") as f:
        data = f.read()
    return Response(
        content=data,
        media_type="application/octet-stream",
        headers={
            "X-Recorded-Sha256": _read_sidecar(sidecar_path) or "",
            "X-Actual-Sha256": sha256_hex(data),
        },
    )


@app.head("/shards/{name}/{version}/{idx}")
def head_shard(name: str, version: int, idx: int):
    _check_drill()
    _validate(name, version, idx)
    shard_path, sidecar_path = _paths(name, version, idx)
    if not os.path.exists(shard_path):
        raise HTTPException(404, "shard not found")
    with open(shard_path, "rb") as f:
        data = f.read()
    return Response(
        status_code=200,
        headers={
            "X-Recorded-Sha256": _read_sidecar(sidecar_path) or "",
            "X-Actual-Sha256": sha256_hex(data),
            "X-Shard-Size": str(len(data)),
        },
    )


@app.get("/admin/drill")
def get_drill():
    with _drill_lock:
        return dict(_drill)


@app.post("/admin/drill")
async def set_drill(request: Request):
    """设置演练开关：{"fail_after_first_store": true|false}。关闭时清除已存片锁存。"""
    body = await request.json()
    enabled = bool(body.get("fail_after_first_store"))
    with _drill_lock:
        _drill["fail_after_first_store"] = enabled
        if not enabled:
            _drill["stored_once"] = False
        return dict(_drill)
