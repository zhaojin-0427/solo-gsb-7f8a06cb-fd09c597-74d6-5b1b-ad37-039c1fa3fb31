"""Coordinator HTTP API.

Responsibilities:

* accept object submits (PUT /objects/{name}) with a request key and an
  expected version;
* split content into two data shards + one XOR parity shard, push shards to
  the three shard nodes and publish a monotonically increasing version only
  after at least two shards are durable *and* the expected version still
  matches the current version;
* serve GET pinned to the version that was current when the request started,
  reconstructing exact bytes from any two trusted shards;
* repair missing/corrupt shards of the *current* version from published
  digests;
* store metadata only - never whole-object content.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import Response

from . import coding
from .nodeclient import (
    STATE_CORRUPT,
    STATE_INTACT,
    STATE_MISSING,
    STATE_UNREACHABLE,
    NodeClient,
)
from .store import (
    AlreadyContinued,
    KeyConflict,
    MetaStore,
    StaleVersion,
    validate_key,
    validate_name,
)


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class Coordinator:
    def __init__(self, db_path: str, node_urls: list[str]):
        self.store = MetaStore(db_path)
        self.node_urls = list(node_urls)
        self._name_locks: dict[str, asyncio.Lock] = {}
        self._name_locks_guard = asyncio.Lock()

    @asynccontextmanager
    async def name_lock(self, name: str):
        async with self._name_locks_guard:
            lock = self._name_locks.get(name)
            if lock is None:
                lock = asyncio.Lock()
                self._name_locks[name] = lock
        async with lock:
            yield

    async def node_map(self, client: NodeClient, write_id: str) -> dict[int, dict]:
        """Probe all three slots concurrently; returns index -> result."""
        results = await asyncio.gather(
            *(client.probe(self.node_urls[i], write_id, i) for i in range(3))
        )
        return {i: results[i] for i in range(3)}

    # ------------------------------------------------------------------
    async def write_phase(
        self, client: NodeClient, write: dict, shards: list[bytes]
    ) -> dict[int, dict]:
        """Ensure every missing/empty slot is stored; return probe-like map.

        Existing intact slots are reused so a lost acknowledgement (node
        crash, drill switch) or coordinator restart never creates a second
        version: we first query what already exists and only continue.
        """
        write_id = write["write_id"]
        probe = await self.node_map(client, write_id)

        async def ensure(index: int) -> dict:
            res = probe[index]
            if res["state"] == STATE_INTACT:
                if res.get("claimed_sha") != write["shard_shas"][index]:
                    return {"state": STATE_CORRUPT, "reason": "digest_mismatch"}
                return res
            if res["state"] == STATE_CORRUPT:
                # Unpublished writes never repair; published repair uses a
                # separate flow with the repair flag.
                return res
            if res["state"] == STATE_MISSING:
                put = await client.put_shard(
                    self.node_urls[index], write_id, index,
                    shards[index], write["shard_shas"][index],
                )
                if put["state"] == STATE_INTACT:
                    return {"state": STATE_INTACT, "claimed_sha": write["shard_shas"][index]}
                if put.get("ack_lost"):
                    # The shard was persisted but its write acknowledgement was
                    # lost (drill switch / 503). Do not count it within THIS
                    # attempt: the client retries with the same request key,
                    # the next attempt's explicit pre-write probe discovers the
                    # shard and continues the same write (never a 2nd version).
                    return {"state": STATE_UNREACHABLE, "ack_lost": True}
                return {"state": STATE_UNREACHABLE}
            return {"state": STATE_UNREACHABLE}

        results = await asyncio.gather(*(ensure(i) for i in range(3)))
        return {i: results[i] for i in range(3)}

    async def submit(
        self,
        client: NodeClient,
        name: str,
        body: bytes,
        request_key: str,
        expected_version: int,
    ) -> dict:
        try:
            validate_name(name)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail={"error": "invalid_name", "message": str(exc)})
        try:
            validate_key(request_key)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail={"error": "invalid_request_key", "message": str(exc)})
        if expected_version < 0:
            raise HTTPException(status_code=422, detail={"error": "invalid_expected_version"})
        try:
            coding.validate_object(body)
        except coding.CodingError as exc:
            raise HTTPException(status_code=422, detail={"error": "invalid_object", "message": str(exc)})

        async with self.name_lock(name):
            shards = coding.split(body)
            whole_sha = sha256_hex(body)
            shard_shas = tuple(sha256_hex(s) for s in shards)
            try:
                prepared = self.store.prepare_or_continue(
                    name=name,
                    request_key=request_key,
                    expected_version=expected_version,
                    whole_sha=whole_sha,
                    shard_shas=shard_shas,
                    shard_len=len(shards[0]),
                    orig_len=len(body),
                )
            except StaleVersion:
                raise HTTPException(
                    status_code=409,
                    detail={
                        "error": "version_conflict",
                        "message": "expected version is no longer the current version",
                    },
                )
            except KeyConflict:
                raise HTTPException(
                    status_code=409,
                    detail={
                        "error": "key_conflict",
                        "message": "request key was already used with different content/object",
                    },
                )
            except AlreadyContinued:
                raise HTTPException(
                    status_code=409,
                    detail={
                        "error": "key_conflict",
                        "message": "request key continues a pending write with different parameters",
                    },
                )

            if prepared["action"] == "published":
                return {
                    "status": "published",
                    "name": name,
                    "version": prepared["version"],
                    "write_id": prepared["write_id"],
                    "whole_sha256": whole_sha,
                    "idempotent_replay": True,
                }

            write = {
                "name": name,
                "version": prepared["version"],
                "write_id": prepared["write_id"],
                "request_key": request_key,
                "expected_version": expected_version,
                "whole_sha": whole_sha,
                "shard_shas": shard_shas,
                "shard_len": len(shards[0]),
                "orig_len": len(body),
            }

            slot_map = await self.write_phase(client, write, shards)
            durable = [i for i, r in slot_map.items() if r["state"] == STATE_INTACT]
            outcome = self.store.try_publish(write, len(durable))

            if outcome.get("published"):
                return {
                    "status": "published",
                    "name": name,
                    "version": outcome["version"],
                    "write_id": write["write_id"],
                    "whole_sha256": whole_sha,
                    "durable_shards": durable,
                    "idempotent_replay": outcome.get("reason") == "already",
                }

            reason = outcome.get("reason")
            if reason in ("stale_expected_version", "superseded"):
                raise HTTPException(
                    status_code=409,
                    detail={
                        "error": "version_conflict",
                        "message": "object changed concurrently; submitted version was not published",
                    },
                )
            # no_quorum / object_gone: published version is untouched.
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "no_quorum",
                    "message": (
                        "fewer than two shard nodes acknowledged durability; "
                        "the published version is unchanged. Retry with the same request key."
                    ),
                    "durable_shards": durable,
                    "write_id": write["write_id"],
                },
            )

    async def get_object(
        self, client: NodeClient, name: str, version: int | None
    ) -> tuple[bytes, dict]:
        try:
            validate_name(name)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail={"error": "invalid_name", "message": str(exc)})
        if version is not None and version <= 0:
            raise HTTPException(status_code=422, detail={"error": "invalid_version"})

        # Pin the version once, at request start; concurrent overwrites use
        # different write_ids, so a pinned read can never mix shard versions.
        obj, vrow = self.store.snapshot(name, version)
        if obj is None:
            raise HTTPException(status_code=404, detail={"error": "object_not_found", "name": name})
        if version is not None and vrow is None:
            raise HTTPException(status_code=404, detail={"error": "version_not_found", "version": version})
        if vrow is None:
            # Object name is known only from an unpublished pending write.
            raise HTTPException(status_code=404, detail={"error": "object_not_found", "name": name})

        write_id = f"w{obj['id']}_v{vrow['version']}"
        expected_shard_shas = (
            vrow["shard0_sha"], vrow["shard1_sha"], vrow["shard2_sha"],
        )
        orig_len = vrow["orig_len"]
        pinned_version = vrow["version"]

        results = await asyncio.gather(
            *(client.get_shard(self.node_urls[i], write_id, i) for i in range(3))
        )
        trusted: dict[int, bytes] = {}
        node_states = []
        for i, res in enumerate(results):
            if res["state"] == STATE_INTACT:
                # Verify against the published per-shard digest.
                if sha256_hex(res["data"]) == expected_shard_shas[i] and len(
                    res["data"]
                ) == vrow["shard_len"]:
                    trusted[i] = res["data"]
                    node_states.append({"index": i, "state": STATE_INTACT})
                else:
                    node_states.append({"index": i, "state": STATE_CORRUPT, "reason": "digest_mismatch"})
            else:
                node_states.append({"index": i, "state": res["state"]})

        if len(trusted) < 2:
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "no_quorum",
                    "message": "fewer than two trusted shards available for this version",
                    "version": pinned_version,
                    "nodes": node_states,
                },
            )

        try:
            data = coding.reconstruct(trusted, orig_len)
        except coding.CodingError as exc:
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "reconstruction_failed",
                    "message": str(exc),
                    "version": pinned_version,
                    "nodes": node_states,
                },
            )

        if sha256_hex(data) != vrow["whole_sha"]:
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "whole_digest_mismatch",
                    "message": "reconstructed bytes do not match the published whole-object digest",
                    "version": pinned_version,
                },
            )

        meta = {
            "name": name,
            "version": pinned_version,
            "write_id": write_id,
            "whole_sha256": vrow["whole_sha"],
            "orig_len": orig_len,
            "nodes": node_states,
        }
        return data, meta

    async def repair(self, client: NodeClient, name: str) -> dict:
        try:
            validate_name(name)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail={"error": "invalid_name", "message": str(exc)})

        async with self.name_lock(name):
            obj, vrow = self.store.snapshot(name)
            if obj is None:
                raise HTTPException(status_code=404, detail={"error": "object_not_found", "name": name})
            if vrow is None:
                raise HTTPException(status_code=404, detail={"error": "object_not_found", "name": name})

            version = vrow["version"]
            write_id = f"w{obj['id']}_v{version}"
            expected_shard_shas = [
                vrow["shard0_sha"], vrow["shard1_sha"], vrow["shard2_sha"],
            ]
            shard_len = vrow["shard_len"]
            orig_len = vrow["orig_len"]

            probe = await self.node_map(client, write_id)
            good: dict[int, bytes] = {}
            bad: list[int] = []
            unknown: list[int] = []
            node_states = []

            for i in range(3):
                res = probe[i]
                if res["state"] == STATE_INTACT and res.get("claimed_sha") == expected_shard_shas[i]:
                    fetched = await client.get_shard(self.node_urls[i], write_id, i)
                    if (
                        fetched["state"] == STATE_INTACT
                        and sha256_hex(fetched["data"]) == expected_shard_shas[i]
                        and len(fetched["data"]) == shard_len
                    ):
                        good[i] = fetched["data"]
                        node_states.append({"index": i, "state": STATE_INTACT})
                        continue
                    res = {"state": STATE_CORRUPT}
                state = res["state"]
                node_states.append({"index": i, "state": state})
                if state in (STATE_MISSING, STATE_CORRUPT):
                    bad.append(i)
                else:  # unreachable / conflict: state cannot be established
                    unknown.append(i)

            if not bad and not unknown:
                return {
                    "status": "healthy",
                    "name": name,
                    "version": version,
                    "write_id": write_id,
                    "nodes": node_states,
                }

            if len(good) < 2:
                raise HTTPException(
                    status_code=503,
                    detail={
                        "error": "no_quorum",
                        "message": "repair needs at least two trusted shards",
                        "defective": bad,
                        "unknown": unknown,
                        "nodes": node_states,
                    },
                )

            # Reconstruct canonical bytes from the trusted shards, then split
            # them again so every repaired byte is validated against digests.
            try:
                whole = coding.reconstruct(good, orig_len)
            except coding.CodingError as exc:
                raise HTTPException(
                    status_code=503,
                    detail={"error": "reconstruction_failed", "message": str(exc)},
                )
            if sha256_hex(whole) != vrow["whole_sha"]:
                raise HTTPException(
                    status_code=503,
                    detail={"error": "whole_digest_mismatch"},
                )
            canonical = coding.split(whole)
            for i, payload in enumerate(canonical):
                if sha256_hex(payload) != expected_shard_shas[i]:
                    raise HTTPException(
                        status_code=503,
                        detail={"error": "published_shard_digest_mismatch", "index": i},
                    )

            repaired = []
            failed = []
            for i in bad:
                # Re-check current version immediately before writing: repair
                # must never overwrite a new version's shards with old ones.
                current_obj, current_vrow = self.store.snapshot(name)
                if (
                    current_obj is None
                    or current_vrow is None
                    or current_obj["current_version"] != version
                ):
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "error": "version_conflict",
                            "message": "object changed during repair; old shards were not written",
                        },
                    )
                put = await client.put_shard(
                    self.node_urls[i], write_id, i, canonical[i],
                    expected_shard_shas[i], repair=True,
                )
                if put["state"] == STATE_INTACT:
                    repaired.append(i)
                else:
                    failed.append({"index": i, "state": put["state"]})

            outcome = {
                "name": name,
                "version": version,
                "write_id": write_id,
                "repaired_shards": repaired,
                "failed": failed,
                "defective": bad,
                "unknown": unknown,
                "nodes": node_states,
            }
            # Every slot of the *current* version must be accounted for;
            # unreachable nodes cannot be declared healthy.
            if unknown or failed:
                outcome["status"] = "unavailable"
                raise HTTPException(
                    status_code=503,
                    detail={"error": "repair_incomplete", **outcome},
                )
            outcome["status"] = "repaired" if repaired else "rechecked"
            return outcome


def default_db_path() -> str:
    return os.environ.get("COORDINATOR_DB", "/data/coordinator.db")


def default_node_urls() -> list[str]:
    return [
        os.environ.get(f"NODE{i}_URL", f"http://node{i}:810{i}")
        for i in range(0, 3)
    ]


def create_app(db_path: str | None = None, node_urls: list[str] | None = None) -> FastAPI:
    from fastapi import FastAPI

    coord = Coordinator(db_path or default_db_path(), node_urls or default_node_urls())

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.coordinator = coord
        app.state.nodes = NodeClient(coord.node_urls)
        try:
            yield
        finally:
            await app.state.nodes.aclose()

    app = FastAPI(title="Object Store Coordinator", version="1.0.0", lifespan=lifespan)

    @app.get("/health")
    async def health():
        checks = await asyncio.gather(
            *(app.state.nodes.health(base) for base in coord.node_urls)
        )
        online = sum(1 for ok in checks if ok)
        return {
            "status": "ok",
            "role": "coordinator",
            "nodes": [
                {"url": url, "online": ok} for url, ok in zip(coord.node_urls, checks)
            ],
            "nodes_online": online,
        }

    @app.put("/objects/{name}")
    async def put_object(
        name: str,
        request: Request,
        x_request_key: str = Header(..., alias="X-Request-Key"),
        x_expected_version: int = Header(..., alias="X-Expected-Version"),
    ):
        body = await request.body()
        result = await coord.submit(
            app.state.nodes, name, body, x_request_key, x_expected_version
        )
        return result

    @app.get("/objects/{name}")
    async def get_object(
        name: str,
        version: int | None = Query(default=None, ge=1),
    ):
        data, meta = await coord.get_object(app.state.nodes, name, version)
        return Response(
            content=data,
            media_type="application/octet-stream",
            headers={
                "X-Object-Version": str(meta["version"]),
                "X-Write-Id": meta["write_id"],
                "X-Whole-SHA256": meta["whole_sha256"],
                "X-Original-Length": str(meta["orig_len"]),
            },
        )

    @app.post("/objects/{name}/repair")
    async def repair_object(name: str):
        return await coord.repair(app.state.nodes, name)

    @app.get("/objects")
    async def list_objects():
        return {"objects": coord.store.list_objects()}

    @app.get("/objects/{name}/versions")
    async def list_versions(name: str):
        try:
            validate_name(name)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail={"error": "invalid_name", "message": str(exc)})
        versions = coord.store.list_versions(name)
        if not versions:
            raise HTTPException(status_code=404, detail={"error": "object_not_found", "name": name})
        return {"name": name, "versions": versions}

    return app

