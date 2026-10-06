"""Shard node HTTP API.

Each node stores immutable shards addressed by ``(write_id, shard_index)``
inside its own SQLite database on an independent persistent volume.

Shard slots are write-once: an intact slot rejects conflicting content with
409. A slot that has been corrupted (its stored bytes no longer hash to the
digest recorded for the slot) can only be overwritten through the repair
path, and only with bytes that match the recorded published digest.
"""

from __future__ import annotations

import hashlib
import os
import re
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import Response
from pydantic import BaseModel

from .coding import SHARD_INDICES, shard_len

MAX_OBJECT_SIZE = 65_536
MAX_SHARD_LEN = shard_len(MAX_OBJECT_SIZE)
WRITE_ID_RE = re.compile(r"^w\d+_v\d+$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class NodeStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.lock = threading.Lock()
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        with self._conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS shards (
                    write_id TEXT NOT NULL,
                    idx INTEGER NOT NULL,
                    claimed_sha TEXT NOT NULL,
                    stored_sha TEXT NOT NULL,
                    data BLOB NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (write_id, idx)
                )
                """
            )
            conn.execute(
                "CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
            )

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    # -- kv / drill switch -------------------------------------------------
    def get_flag(self, key: str, default: str = "0") -> str:
        with self._conn() as conn:
            row = conn.execute("SELECT value FROM kv WHERE key = ?", (key,)).fetchone()
            return row["value"] if row else default

    def set_flag(self, key: str, value: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO kv(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )

    # -- shard operations --------------------------------------------------
    def get_slot(self, write_id: str, idx: int) -> sqlite3.Row | None:
        with self._conn() as conn:
            return conn.execute(
                "SELECT * FROM shards WHERE write_id = ? AND idx = ?", (write_id, idx)
            ).fetchone()

    def list_slots(self, write_id: str) -> list[sqlite3.Row]:
        with self._conn() as conn:
            return list(
                conn.execute("SELECT * FROM shards WHERE write_id = ? ORDER BY idx", (write_id,))
            )

    def list_all(self) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT write_id, idx, claimed_sha, stored_sha FROM shards ORDER BY write_id, idx"
            ).fetchall()
            return [dict(r) for r in rows]

    def put_shard(
        self, write_id: str, idx: int, claimed_sha: str, data: bytes, repair: bool
    ) -> tuple[str, sqlite3.Row]:
        """Return ``(status, row)`` where status is stored/exists/repaired.

        Raises HTTPException(409) when an immutable intact slot holds other
        content, or when a corrupt slot is addressed outside the repair path.
        """
        now = datetime.now(timezone.utc).isoformat()
        with self.lock, self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM shards WHERE write_id = ? AND idx = ?", (write_id, idx)
            ).fetchone()
            if row is not None:
                intact = row["stored_sha"] == row["claimed_sha"]
                if intact:
                    if row["claimed_sha"] == claimed_sha and row["stored_sha"] == sha256_hex(data):
                        return "exists", row
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "error": "immutable_shard",
                            "message": "slot already stores a different immutable shard",
                            "write_id": write_id,
                            "index": idx,
                        },
                    )
                # Corrupt slot: repair is the only allowed path.
                if not repair:
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "error": "corrupt_shard_needs_repair",
                            "message": "slot is corrupt; it may only be rewritten via repair",
                            "write_id": write_id,
                            "index": idx,
                        },
                    )
                if claimed_sha != row["claimed_sha"] or sha256_hex(data) != claimed_sha:
                    raise HTTPException(
                        status_code=422,
                        detail={
                            "error": "repair_digest_mismatch",
                            "message": "repair payload must match the published digest for this slot",
                        },
                    )
                conn.execute(
                    "UPDATE shards SET stored_sha = ?, data = ?, updated_at = ? "
                    "WHERE write_id = ? AND idx = ?",
                    (claimed_sha, data, now, write_id, idx),
                )
                row = conn.execute(
                    "SELECT * FROM shards WHERE write_id = ? AND idx = ?", (write_id, idx)
                ).fetchone()
                return "repaired", row

            # Empty slot.
            if sha256_hex(data) != claimed_sha:
                raise HTTPException(
                    status_code=422,
                    detail={
                        "error": "digest_mismatch",
                        "message": "X-Shard-SHA256 does not match the uploaded body",
                    },
                )
            conn.execute(
                "INSERT INTO shards(write_id, idx, claimed_sha, stored_sha, data, "
                "created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?)",
                (write_id, idx, claimed_sha, claimed_sha, data, now, now),
            )
            row = conn.execute(
                "SELECT * FROM shards WHERE write_id = ? AND idx = ?", (write_id, idx)
            ).fetchone()
            return "stored", row

    def corrupt_shard(self, write_id: str, idx: int) -> None:
        with self.lock, self._conn() as conn:
            row = conn.execute(
                "SELECT data FROM shards WHERE write_id = ? AND idx = ?", (write_id, idx)
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="shard not found")
            data = row["data"]
            flipped = bytes([data[0] ^ 0xFF]) + data[1:] if data else b"\xff"
            now = datetime.now(timezone.utc).isoformat()
            conn.execute(
                "UPDATE shards SET data = ?, stored_sha = ?, updated_at = ? "
                "WHERE write_id = ? AND idx = ?",
                (flipped, sha256_hex(flipped), now, write_id, idx),
            )


# --------------------------------------------------------------------------
# Application factory
# --------------------------------------------------------------------------

class DrillBody(BaseModel):
    armed: bool


def create_node_app(db_path: str, node_name: str = "node") -> FastAPI:
    app = FastAPI(title=f"Shard Node ({node_name})", version="1.0.0")
    app.state.node_name = node_name
    store = NodeStore(db_path)
    app.state.store = store

    def validate_write_id(write_id: str) -> None:
        if not WRITE_ID_RE.match(write_id):
            raise HTTPException(
                status_code=422,
                detail={"error": "invalid_write_id", "write_id": write_id},
            )

    @app.get("/health")
    async def health():
        return {"status": "ok", "role": "node", "node": node_name}

    @app.put("/shards/{write_id}/{idx}")
    async def put_shard(
        write_id: str,
        idx: int,
        request: Request,
        repair: bool = Query(False),
        x_shard_sha256: str = Header(..., alias="X-Shard-SHA256"),
    ):
        validate_write_id(write_id)
        if idx not in SHARD_INDICES:
            raise HTTPException(
                status_code=422,
                detail={"error": "invalid_shard_index", "index": idx},
            )
        if not SHA256_RE.match(x_shard_sha256):
            raise HTTPException(
                status_code=422,
                detail={"error": "invalid_sha_digest", "header": "X-Shard-SHA256"},
            )
        data = await request.body()
        if not (1 <= len(data) <= MAX_SHARD_LEN):
            raise HTTPException(
                status_code=422,
                detail={
                    "error": "invalid_shard_length",
                    "length": len(data),
                    "max": MAX_SHARD_LEN,
                },
            )

        # Persist first; the drill switch simulates an acknowledgement lost
        # right after a successful first store. Reads and idempotent retries
        # stay unaffected: the coordinator probes slots before storing.
        status, row = store.put_shard(write_id, idx, x_shard_sha256, data, repair)
        drill_armed = store.get_flag("drill_armed") == "1"
        if drill_armed and status == "stored":
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "drill_store_ack_lost",
                    "message": "drill switch: shard persisted but acknowledgement lost",
                    "write_id": write_id,
                    "index": idx,
                },
            )
        return {
            "status": status,
            "write_id": write_id,
            "index": idx,
            "claimed_sha256": row["claimed_sha"],
            "stored_sha256": row["stored_sha"],
            "length": len(row["data"]),
            "drill_armed": drill_armed,
        }

    @app.get("/shards/{write_id}/{idx}")
    async def get_shard(write_id: str, idx: int):
        validate_write_id(write_id)
        if idx not in SHARD_INDICES:
            raise HTTPException(status_code=422, detail={"error": "invalid_shard_index"})
        row = store.get_slot(write_id, idx)
        if row is None:
            raise HTTPException(status_code=404, detail="shard not found")
        if row["stored_sha"] != row["claimed_sha"]:
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "corrupt_shard",
                    "write_id": write_id,
                    "index": idx,
                    "claimed_sha256": row["claimed_sha"],
                    "stored_sha256": row["stored_sha"],
                },
            )
        return Response(content=row["data"], media_type="application/octet-stream")

    @app.get("/shards/{write_id}/{idx}/meta")
    async def get_shard_meta(write_id: str, idx: int):
        validate_write_id(write_id)
        if idx not in SHARD_INDICES:
            raise HTTPException(status_code=422, detail={"error": "invalid_shard_index"})
        row = store.get_slot(write_id, idx)
        if row is None:
            raise HTTPException(status_code=404, detail="shard not found")
        intact = row["stored_sha"] == row["claimed_sha"]
        return {
            "write_id": write_id,
            "index": idx,
            "length": len(row["data"]),
            "claimed_sha256": row["claimed_sha"],
            "stored_sha256": row["stored_sha"],
            "intact": intact,
        }

    @app.get("/writes/{write_id}")
    async def list_write_shards(write_id: str):
        validate_write_id(write_id)
        rows = store.list_slots(write_id)
        return {
            "write_id": write_id,
            "shards": [
                {
                    "index": r["idx"],
                    "claimed_sha256": r["claimed_sha"],
                    "stored_sha256": r["stored_sha"],
                    "intact": r["stored_sha"] == r["claimed_sha"],
                    "length": len(r["data"]),
                }
                for r in rows
            ],
        }

    # -- drill / maintenance ------------------------------------------------
    @app.get("/admin/drill")
    async def get_drill():
        return {"armed": store.get_flag("drill_armed") == "1"}

    @app.post("/admin/drill")
    async def set_drill(body: DrillBody):
        store.set_flag("drill_armed", "1" if body.armed else "0")
        return {"armed": body.armed}

    @app.post("/admin/shards/{write_id}/{idx}/corrupt")
    async def corrupt_shard(write_id: str, idx: int):
        validate_write_id(write_id)
        if idx not in SHARD_INDICES:
            raise HTTPException(status_code=422, detail={"error": "invalid_shard_index"})
        store.corrupt_shard(write_id, idx)
        return {"status": "corrupted", "write_id": write_id, "index": idx}

    @app.get("/admin/shards")
    async def admin_list():
        return {"node": node_name, "shards": store.list_all()}

    return app
