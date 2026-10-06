"""Async HTTP client used by the coordinator to talk to shard nodes."""

from __future__ import annotations

import httpx

STATE_INTACT = "intact"
STATE_CORRUPT = "corrupt"
STATE_MISSING = "missing"
STATE_UNREACHABLE = "unreachable"
STATE_CONFLICT = "conflict"


class NodeClient:
    def __init__(self, nodes: list[str], timeout: float = 3.0):
        self.nodes = list(nodes)
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout, connect=timeout),
            trust_env=False,
        )

    async def aclose(self) -> None:
        await self.client.aclose()

    async def __aenter__(self) -> "NodeClient":
        return self

    async def __aexit__(self, *exc) -> None:
        await self.aclose()

    async def health(self, base: str) -> bool:
        try:
            r = await self.client.get(f"{base}/health")
            return r.status_code == 200
        except httpx.HTTPError:
            return False

    async def probe(self, base: str, write_id: str, index: int) -> dict:
        """Inspect a slot without moving its bytes."""
        try:
            r = await self.client.get(f"{base}/shards/{write_id}/{index}/meta")
        except httpx.HTTPError:
            return {"state": STATE_UNREACHABLE}
        if r.status_code == 200:
            body = r.json()
            return {
                "state": STATE_INTACT if body.get("intact") else STATE_CORRUPT,
                "claimed_sha": body.get("claimed_sha256"),
                "stored_sha": body.get("stored_sha256"),
                "length": body.get("length"),
            }
        if r.status_code == 404:
            return {"state": STATE_MISSING}
        if r.status_code == 422:
            return {"state": STATE_CONFLICT}
        return {"state": STATE_UNREACHABLE}

    async def put_shard(
        self, base: str, write_id: str, index: int, data: bytes,
        claimed_sha: str, repair: bool = False,
    ) -> dict:
        try:
            r = await self.client.put(
                f"{base}/shards/{write_id}/{index}",
                params={"repair": "true"} if repair else {},
                content=data,
                headers={
                    "X-Shard-SHA256": claimed_sha,
                    "Content-Type": "application/octet-stream",
                },
            )
        except httpx.HTTPError:
            return {"state": STATE_UNREACHABLE}
        if r.status_code in (200, 201):
            body = r.json()
            return {
                "state": STATE_INTACT,
                "status": body.get("status"),
                "stored_sha": body.get("stored_sha256"),
            }
        if r.status_code == 409:
            return {"state": STATE_CONFLICT, "detail": _detail(r)}
        if r.status_code == 503:
            # Drill ack-loss (shard persisted) or a genuinely failing node;
            # a follow-up probe disambiguates.
            return {"state": STATE_UNREACHABLE, "ack_lost": True, "detail": _detail(r)}
        return {"state": STATE_UNREACHABLE, "status_code": r.status_code}

    async def get_shard(self, base: str, write_id: str, index: int) -> dict:
        try:
            r = await self.client.get(
                f"{base}/shards/{write_id}/{index}",
            )
        except httpx.HTTPError:
            return {"state": STATE_UNREACHABLE}
        if r.status_code == 200:
            return {"state": STATE_INTACT, "data": r.content}
        if r.status_code == 404:
            return {"state": STATE_MISSING}
        if r.status_code == 503:
            return {"state": STATE_CORRUPT, "detail": _detail(r)}
        return {"state": STATE_UNREACHABLE, "status_code": r.status_code}


def _detail(r: httpx.Response):
    try:
        return r.json().get("detail")
    except Exception:
        return r.text
