"""Coordinator metadata store (SQLite).

The coordinator persists only metadata: object names, versions, digests
(original/whole and per-shard) and request-key bookkeeping. It never keeps
object bytes; shard bytes live exclusively on the shard nodes.

All accesses are serialized with an in-process lock. The deployment runs a
single coordinator worker (SQLite file on local disk), which is the intended
topology for this service.
"""

from __future__ import annotations

import os
import re
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone

NAME_RE = re.compile(r"^[A-Za-z0-9._-]{1,255}\Z")
KEY_RE = re.compile(r"^[A-Za-z0-9._:\-]{1,128}\Z")

STATUS_PENDING = "pending"
STATUS_PUBLISHED = "published"


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def validate_name(name: str) -> None:
    if not isinstance(name, str) or not NAME_RE.match(name):
        raise ValueError(
            "object name must be 1-255 chars from [A-Za-z0-9._-]"
        )


def validate_key(key: str) -> None:
    if not isinstance(key, str) or not KEY_RE.match(key):
        raise ValueError(
            "request key must be 1-128 chars from [A-Za-z0-9._:-]"
        )


class StaleVersion(Exception):
    """Expected version is no longer the current version."""


class KeyConflict(Exception):
    """Request key already used with different content/name."""


class AlreadyContinued(Exception):
    """Key belongs to a pending write whose parameters disagree."""


class MetaStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.lock = threading.RLock()
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        with self.connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS objects (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    current_version INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS versions (
                    name TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    orig_len INTEGER NOT NULL,
                    shard_len INTEGER NOT NULL,
                    whole_sha TEXT NOT NULL,
                    shard0_sha TEXT NOT NULL,
                    shard1_sha TEXT NOT NULL,
                    shard2_sha TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (name, version)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS writes (
                    name TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    write_id TEXT NOT NULL UNIQUE,
                    request_key TEXT NOT NULL,
                    expected_version INTEGER NOT NULL,
                    whole_sha TEXT NOT NULL,
                    shard0_sha TEXT NOT NULL,
                    shard1_sha TEXT NOT NULL,
                    shard2_sha TEXT NOT NULL,
                    shard_len INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (name, version)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS request_keys (
                    request_key TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    expected_version INTEGER NOT NULL,
                    whole_sha TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(self.db_path, timeout=15, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=15000")
        try:
            yield conn
        finally:
            conn.close()

    # ------------------------------------------------------------------
    def get_object(self, conn, name: str):
        return conn.execute("SELECT * FROM objects WHERE name = ?", (name,)).fetchone()

    def get_version(self, conn, name: str, version: int):
        return conn.execute(
            "SELECT * FROM versions WHERE name = ? AND version = ?", (name, version)
        ).fetchone()

    def get_write(self, conn, name: str, version: int):
        return conn.execute(
            "SELECT * FROM writes WHERE name = ? AND version = ?", (name, version)
        ).fetchone()

    def get_key(self, conn, request_key: str):
        return conn.execute(
            "SELECT * FROM request_keys WHERE request_key = ?", (request_key,)
        ).fetchone()

    def list_objects(self):
        with self.lock, self.connect() as conn:
            return [
                dict(r)
                for r in conn.execute(
                    "SELECT name, current_version, created_at, updated_at "
                    "FROM objects WHERE current_version > 0 ORDER BY name"
                )
            ]

    def snapshot(self, name: str, version: int | None = None):
        """Return (object_row, version_row) consistently, or None rows."""
        with self.lock, self.connect() as conn:
            conn.execute("BEGIN")
            obj = self.get_object(conn, name)
            if obj is None or obj["current_version"] == 0:
                conn.execute("ROLLBACK")
                return None, None
            ver = version if version is not None else obj["current_version"]
            vrow = self.get_version(conn, name, ver)
            conn.execute("COMMIT")
            return obj, vrow

    def prepare_or_continue(
        self,
        name: str,
        request_key: str,
        expected_version: int,
        whole_sha: str,
        shard_shas: tuple[str, str, str],
        shard_len: int,
        orig_len: int,
    ) -> dict:
        """Resolve a submit request against request-key / version state.

        Returns a dict describing either an existing published write
        (``action='published'``), a pending write to continue
        (``action='continue'``) or a brand-new pending write
        (``action='prepared'``).
        """
        validate_name(name)
        validate_key(request_key)
        with self.lock, self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            obj = self.get_object(conn, name)
            current = obj["current_version"] if obj is not None else 0
            obj_id = obj["id"] if obj is not None else None

            key_row = self.get_key(conn, request_key)
            if key_row is not None:
                if key_row["name"] != name:
                    conn.execute("ROLLBACK")
                    raise KeyConflict("request key is already used for another object")

                if key_row["status"] == STATUS_PUBLISHED:
                    if key_row["whole_sha"] != whole_sha:
                        conn.execute("ROLLBACK")
                        raise KeyConflict(
                            "request key already published with different content"
                        )
                    vrow = self.get_version(conn, name, key_row["version"])
                    conn.execute("COMMIT")
                    return {
                        "action": "published",
                        "write_id": f"w{obj_id}_v{key_row['version']}",
                        "version": key_row["version"],
                        "whole_sha": whole_sha,
                        "shard_shas": (
                            vrow["shard0_sha"], vrow["shard1_sha"], vrow["shard2_sha"],
                        ),
                        "shard_len": vrow["shard_len"],
                        "orig_len": vrow["orig_len"],
                    }

                # Pending: continue the same write.
                pending = self.get_write(conn, name, key_row["version"])
                if (
                    pending["whole_sha"] != whole_sha
                    or pending["expected_version"] != expected_version
                    or key_row["expected_version"] != expected_version
                ):
                    conn.execute("ROLLBACK")
                    raise AlreadyContinued(
                        "request key continues a pending write with different parameters"
                    )
                if current != expected_version:
                    conn.execute("ROLLBACK")
                    raise StaleVersion("expected version is no longer the current version")
                conn.execute(
                    "UPDATE request_keys SET updated_at = ? WHERE request_key = ?",
                    (now(), request_key),
                )
                conn.execute(
                    "UPDATE writes SET updated_at = ? WHERE name = ? AND version = ?",
                    (now(), name, key_row["version"]),
                )
                conn.execute("COMMIT")
                return {
                    "action": "continue",
                    "write_id": pending["write_id"],
                    "version": pending["version"],
                    "whole_sha": whole_sha,
                    "shard_shas": (
                        pending["shard0_sha"], pending["shard1_sha"], pending["shard2_sha"],
                    ),
                    "shard_len": pending["shard_len"],
                    "orig_len": orig_len,
                    "expected_version": expected_version,
                }

            # Brand-new request key. It must not race an unfinished write
            # that already reserved the next version of this name; only that
            # write's request key may continue it.
            existing_pending = conn.execute(
                "SELECT request_key FROM writes WHERE name = ? AND status = ?",
                (name, STATUS_PENDING),
            ).fetchone()
            if existing_pending is not None:
                conn.execute("ROLLBACK")
                raise AlreadyContinued(
                    "an unpublished write is in progress for this object; it can only "
                    "be continued with its original request key "
                    f"({existing_pending['request_key']})"
                )

            if current != expected_version:
                conn.execute("ROLLBACK")
                raise StaleVersion("expected version is no longer the current version")

            ts = now()
            if obj is None:
                cur = conn.execute(
                    "INSERT INTO objects(name, current_version, created_at, updated_at) "
                    "VALUES(?, 0, ?, ?)",
                    (name, ts, ts),
                )
                obj_id = cur.lastrowid
            next_version = current + 1
            write_id = f"w{obj_id}_v{next_version}"
            conn.execute(
                "INSERT INTO writes(name, version, write_id, request_key, expected_version, "
                "whole_sha, shard0_sha, shard1_sha, shard2_sha, shard_len, status, "
                "created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    name, next_version, write_id, request_key, expected_version,
                    whole_sha, *shard_shas, shard_len, STATUS_PENDING, ts, ts,
                ),
            )
            conn.execute(
                "INSERT INTO request_keys(request_key, name, version, expected_version, "
                "whole_sha, status, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    request_key, name, next_version, expected_version, whole_sha,
                    STATUS_PENDING, ts, ts,
                ),
            )
            conn.execute("COMMIT")
            return {
                "action": "prepared",
                "write_id": write_id,
                "version": next_version,
                "whole_sha": whole_sha,
                "shard_shas": shard_shas,
                "shard_len": shard_len,
                "orig_len": orig_len,
                "expected_version": expected_version,
            }

    def try_publish(self, write: dict, durable_count: int) -> dict:
        """Attempt to publish the pending write if quorum and version hold.

        Returns ``{"published": bool, "version": int, "reason": str}``.
        """
        name = write["name"]
        version = write["version"]
        with self.lock, self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            obj = self.get_object(conn, name)
            if obj is None:
                conn.execute("ROLLBACK")
                return {"published": False, "reason": "object_gone"}
            if obj["current_version"] >= version:
                # Already published (lost-ack retry) or superseded.
                vrow = self.get_version(conn, name, version)
                conn.execute("ROLLBACK")
                if vrow is not None and vrow["whole_sha"] == write["whole_sha"]:
                    return {"published": True, "version": version, "reason": "already"}
                return {"published": False, "reason": "superseded"}
            if obj["current_version"] != write["expected_version"]:
                conn.execute("ROLLBACK")
                return {"published": False, "reason": "stale_expected_version"}
            if durable_count < 2:
                conn.execute("ROLLBACK")
                return {"published": False, "reason": "no_quorum"}

            ts = now()
            conn.execute(
                "INSERT INTO versions(name, version, orig_len, shard_len, whole_sha, "
                "shard0_sha, shard1_sha, shard2_sha, created_at) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    name, version, write["orig_len"], write["shard_len"],
                    write["whole_sha"], *write["shard_shas"], ts,
                ),
            )
            conn.execute(
                "UPDATE objects SET current_version = ?, updated_at = ? WHERE name = ?",
                (version, ts, name),
            )
            conn.execute(
                "UPDATE writes SET status = ?, updated_at = ? WHERE name = ? AND version = ?",
                (STATUS_PUBLISHED, ts, name, version),
            )
            conn.execute(
                "UPDATE request_keys SET status = ?, updated_at = ? "
                "WHERE name = ? AND version = ?",
                (STATUS_PUBLISHED, ts, name, version),
            )
            conn.execute("COMMIT")
            return {"published": True, "version": version, "reason": "published"}

    def current_version_unchanged(self, name: str, expected_version: int) -> bool:
        with self.lock, self.connect() as conn:
            conn.execute("BEGIN")
            obj = self.get_object(conn, name)
            cur = obj["current_version"] if obj else 0
            conn.execute("COMMIT")
            return cur == expected_version

    def list_versions(self, name: str):
        with self.lock, self.connect() as conn:
            rows = conn.execute(
                "SELECT version, orig_len, whole_sha, created_at FROM versions "
                "WHERE name = ? ORDER BY version",
                (name,),
            ).fetchall()
            return [dict(r) for r in rows]
