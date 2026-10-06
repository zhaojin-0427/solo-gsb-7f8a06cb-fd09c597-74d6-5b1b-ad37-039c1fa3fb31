"""End-to-end tests over real HTTP processes.

Spins up one coordinator + three shard-node uvicorn servers on localhost,
then exercises quorum writes, lost-acknowledgement continuation, fixed-version
reads, idempotent retry, conflicts, corruption repair and validation errors.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait(url: str, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            if httpx.get(url, timeout=1.0).status_code == 200:
                return
        except Exception as exc:  # noqa: BLE001
            last = exc
        time.sleep(0.2)
    raise RuntimeError(f"server did not become ready: {url} ({last})")


class Cluster:
    def __init__(self, tmpdir: Path):
        self.tmpdir = tmpdir
        self.node_procs: list[subprocess.Popen | None] = [None, None, None]
        self.coord_proc: subprocess.Popen | None = None
        self.node_ports = [free_port() for _ in range(3)]
        self.coord_port = free_port()
        self.node_urls = [f"http://127.0.0.1:{p}" for p in self.node_ports]
        self.coord_url = f"http://127.0.0.1:{self.coord_port}"

    def _spawn(self, env: dict) -> subprocess.Popen:
        proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.main:app_factory", "--factory",
             "--host", "127.0.0.1", "--port", str(env["PORT"])],
            cwd=ROOT,
            env={**os.environ, **env},
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return proc

    def start(self) -> None:
        for i, port in enumerate(self.node_ports):
            self.node_procs[i] = self._spawn({
                "SERVICE_ROLE": "node",
                "NODE_NAME": f"node{i}",
                "PORT": str(port),
                "DB_PATH": str(self.tmpdir / f"node{i}.db"),
            })
        env = {
            "SERVICE_ROLE": "coordinator",
            "PORT": str(self.coord_port),
            "DB_PATH": str(self.tmpdir / "coord.db"),
        }
        for i, url in enumerate(self.node_urls):
            env[f"NODE{i}_URL"] = url
        self.coord_proc = self._spawn(env)
        for url in self.node_urls:
            _wait(f"{url}/health")
        _wait(f"{self.coord_url}/health")

    def stop(self) -> None:
        procs = [p for p in self.node_procs if p is not None]
        if self.coord_proc is not None:
            procs.append(self.coord_proc)
        for proc in procs:
            proc.terminate()
        for proc in procs:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        self.node_procs = [None, None, None]
        self.coord_proc = None

    def restart_coordinator(self) -> None:
        assert self.coord_proc is not None
        self.coord_proc.terminate()
        self.coord_proc.wait(timeout=5)
        env = {
            "SERVICE_ROLE": "coordinator",
            "PORT": str(self.coord_port),
            "DB_PATH": str(self.tmpdir / "coord.db"),
        }
        for i, url in enumerate(self.node_urls):
            env[f"NODE{i}_URL"] = url
        self.coord_proc = self._spawn(env)
        _wait(f"{self.coord_url}/health")

    def stop_node(self, index: int) -> None:
        proc = self.node_procs[index]
        assert proc is not None
        proc.terminate()
        proc.wait(timeout=5)
        self.node_procs[index] = None

    def start_node(self, index: int) -> None:
        port = self.node_ports[index]
        self.node_procs[index] = self._spawn({
            "SERVICE_ROLE": "node",
            "NODE_NAME": f"node{index}",
            "PORT": str(port),
            "DB_PATH": str(self.tmpdir / f"node{index}.db"),
        })
        _wait(f"{self.node_urls[index]}/health")


@pytest.fixture()
def cluster(tmp_path: Path):
    c = Cluster(tmp_path)
    c.start()
    try:
        yield c
    finally:
        c.stop()


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def submit(client: httpx.Client, cluster: Cluster, name: str, data: bytes,
           key: str, expected: int) -> httpx.Response:
    return client.put(
        f"{cluster.coord_url}/objects/{name}",
        content=data,
        headers={"X-Request-Key": key, "X-Expected-Version": str(expected)},
    )


def get(client: httpx.Client, cluster: Cluster, name: str,
        version: int | None = None) -> httpx.Response:
    url = f"{cluster.coord_url}/objects/{name}"
    if version is not None:
        url += f"?version={version}"
    return client.get(url)


# ---------------------------------------------------------------------------
# coding unit checks
# ---------------------------------------------------------------------------

def test_coding_roundtrip_odd_and_even():
    from app import coding

    for data in [b"a", b"ab", b"abc", bytes(range(251)) + b"zz", b"\x00" * 65536]:
        shards = coding.split(data)
        assert all(len(s) == coding.shard_len(len(data)) for s in shards)
        for pair in ((0, 1), (0, 2), (1, 2)):
            subset = {i: shards[i] for i in pair}
            assert coding.reconstruct(subset, len(data)) == data


def test_coding_rejects_size():
    from app import coding

    with pytest.raises(coding.CodingError):
        coding.split(b"")
    with pytest.raises(coding.CodingError):
        coding.split(b"x" * 65537)


def test_coding_parity_mismatch_detected():
    from app import coding

    # all three shards present but mutually inconsistent -> parity check fails
    with pytest.raises(coding.CodingError):
        coding.reconstruct({0: b"aaaa", 1: b"bbbb", 2: b"cccc"}, 8)
    # wrong length against recorded orig_len half
    with pytest.raises(coding.CodingError):
        coding.reconstruct({0: b"aa", 1: b"bb"}, 5)


# ---------------------------------------------------------------------------
# happy path + versioning
# ---------------------------------------------------------------------------

def test_submit_and_get(cluster: Cluster):
    data = b"hello erasure world"
    with httpx.Client(timeout=10) as c:
        r = submit(c, cluster, "doc1", data, "key-1", 0)
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["version"] == 1
        assert body["whole_sha256"] == sha(data)
        assert len(body["durable_shards"]) >= 2

        r = get(c, cluster, "doc1")
        assert r.status_code == 200
        assert r.content == data
        assert r.headers["x-object-version"] == "1"
        assert r.headers["x-whole-sha256"] == sha(data)

        # overwrite at expected version 1
        data2 = b"second version payload!"
        r = submit(c, cluster, "doc2-and-doc1", data2, "unused", 0)  # ensure no collision
        assert r.status_code == 200
        r = submit(c, cluster, "doc1", data2, "key-2", 1)
        assert r.status_code == 200
        assert r.json()["version"] == 2

        # GET without version pins to the newest at request start
        assert get(c, cluster, "doc1").content == data2
        # old version still readable byte-exact
        r = get(c, cluster, "doc1", version=1)
        assert r.status_code == 200
        assert r.content == data
        assert r.headers["x-object-version"] == "1"


def test_odd_length_roundtrip_over_http(cluster: Cluster):
    for i, size in enumerate((1, 2, 3, 65535, 65536)):
        payload = bytes((j * 7 + i) % 256 for j in range(size))
        with httpx.Client(timeout=15) as c:
            r = submit(c, cluster, f"obj-size-{size}", payload, f"size-key-{size}", 0)
            assert r.status_code == 200, r.text
            r = get(c, cluster, f"obj-size-{size}")
            assert r.status_code == 200
            assert r.content == payload
            assert r.headers["x-original-length"] == str(size)


# ---------------------------------------------------------------------------
# failure tolerance
# ---------------------------------------------------------------------------

def test_one_node_offline_write_and_read(cluster: Cluster):
    data = b"survive one node down" * 4
    cluster.stop_node(1)
    try:
        with httpx.Client(timeout=10) as c:
            r = submit(c, cluster, "resilient", data, "r-key-1", 0)
            assert r.status_code == 200, r.text
            assert sorted(r.json()["durable_shards"]) == [0, 2]

            r = get(c, cluster, "resilient")
            assert r.status_code == 200
            assert r.content == data
    finally:
        cluster.start_node(1)

    # after the node returns, repair should refill its missing shard
    with httpx.Client(timeout=10) as c:
        r = c.post(f"{cluster.coord_url}/objects/resilient/repair")
        assert r.status_code == 200
        assert set(r.json()["repaired_shards"]) == {1}
        cluster.stop_node(0)
        try:
            r = get(c, cluster, "resilient")
            assert r.status_code == 200
            assert r.content == data
        finally:
            cluster.start_node(0)


def test_repair_against_unreachable_node_returns_503(cluster: Cluster):
    data = b"repair while a node is down must not report healthy"
    with httpx.Client(timeout=10) as c:
        r = submit(c, cluster, "rdown", data, "rd-key", 0)
        assert r.status_code == 200
    cluster.stop_node(1)
    try:
        with httpx.Client(timeout=10) as c:
            r = c.post(f"{cluster.coord_url}/objects/rdown/repair")
            assert r.status_code == 503
            detail = r.json()["detail"]
            assert detail["error"] == "repair_incomplete"
            assert detail["unknown"] == [1]
    finally:
        cluster.start_node(1)
    with httpx.Client(timeout=10) as c:
        r = c.post(f"{cluster.coord_url}/objects/rdown/repair")
        assert r.status_code == 200
        assert r.json()["status"] in ("repaired", "rechecked", "healthy")


def test_two_nodes_offline_returns_503_and_does_not_publish(cluster: Cluster):
    data = b"only one node up"
    cluster.stop_node(1)
    cluster.stop_node(2)
    try:
        with httpx.Client(timeout=10) as c:
            r = submit(c, cluster, "underreplicated", data, "u-key-1", 0)
            assert r.status_code == 503
            assert "durable_shards" in r.json()["detail"]

            # published version must not have moved (object still 404)
            r = get(c, cluster, "underreplicated")
            assert r.status_code == 404
    finally:
        cluster.start_node(2)
        cluster.start_node(1)

    # same key continues the unfinished write: no second version
    with httpx.Client(timeout=10) as c:
        r = submit(c, cluster, "underreplicated", data, "u-key-1", 0)
        assert r.status_code == 200, r.text
        assert r.json()["version"] == 1
        assert get(c, cluster, "underreplicated").content == data


def test_get_fewer_than_two_trusted_returns_503(cluster: Cluster):
    data = b"readable then degraded"
    with httpx.Client(timeout=10) as c:
        r = submit(c, cluster, "degrade", data, "d-key-1", 0)
        assert r.status_code == 200
        write_id = r.json()["write_id"]

    cluster.stop_node(1)
    try:
        # corrupt shard 0 directly on node0
        with httpx.Client(timeout=10) as c:
            r = c.post(f"{cluster.node_urls[0]}/admin/shards/{write_id}/0/corrupt")
            assert r.status_code == 200
            r = get(c, cluster, "degrade")
            assert r.status_code == 503
            assert r.json()["detail"]["error"] in ("no_quorum",)
    finally:
        cluster.start_node(1)


# ---------------------------------------------------------------------------
# idempotency / conflicts
# ---------------------------------------------------------------------------

def test_published_same_name_key_content_returns_original_version(cluster: Cluster):
    data = b"idempotent content"
    with httpx.Client(timeout=10) as c:
        r1 = submit(c, cluster, "idem", data, "idem-key", 0)
        assert r1.status_code == 200
        r2 = submit(c, cluster, "idem", data, "idem-key", 0)
        assert r2.status_code == 200
        assert r2.json()["version"] == 1
        assert r2.json()["idempotent_replay"] is True


def test_same_key_different_content_is_409(cluster: Cluster):
    with httpx.Client(timeout=10) as c:
        assert submit(c, cluster, "conf", b"content A", "k", 0).status_code == 200
        r = submit(c, cluster, "conf", b"content B!!", "k", 0)
        assert r.status_code == 409


def test_stale_expected_version_is_409(cluster: Cluster):
    with httpx.Client(timeout=10) as c:
        assert submit(c, cluster, "stale", b"v1", "sk1", 0).status_code == 200
        assert submit(c, cluster, "stale", b"v2", "sk2", 1).status_code == 200
        r = submit(c, cluster, "stale", b"v3-oldbase", "sk3", 1)
        assert r.status_code == 409


def test_pending_other_key_same_name_is_409(cluster: Cluster):
    cluster.stop_node(1)
    cluster.stop_node(2)
    try:
        with httpx.Client(timeout=10) as c:
            r = submit(c, cluster, "pblock", b"x", "pk-original", 0)
            assert r.status_code == 503
            r = submit(c, cluster, "pblock", b"x", "pk-other", 0)
            assert r.status_code == 409
    finally:
        cluster.start_node(2)
        cluster.start_node(1)


def test_concurrent_overwrites_one_wins_no_mixed_shards(cluster: Cluster):
    with httpx.Client(timeout=10) as c:
        assert submit(c, cluster, "race", b"initial", "race-0", 0).status_code == 200

    async def push(key: str, payload: bytes):
        async with httpx.AsyncClient(timeout=10) as ac:
            return await ac.put(
                f"{cluster.coord_url}/objects/race",
                content=payload,
                headers={"X-Request-Key": key, "X-Expected-Version": "1"},
            )

    async def run():
        return await asyncio.gather(
            push("race-a", b"A" * 40), push("race-b", b"B" * 40)
        )

    responses = asyncio.run(run())
    statuses = {r.status_code for r in responses}
    assert statuses == {200, 409}
    winner_payload = b"A" * 40 if responses[0].status_code == 200 else b"B" * 40
    with httpx.Client(timeout=10) as c:
        r = get(c, cluster, "race")
        assert r.status_code == 200
        assert r.content == winner_payload  # exact bytes => no shard mixing


# ---------------------------------------------------------------------------
# drill switch: shard stored then 503; continuation must not fork versions
# ---------------------------------------------------------------------------

def test_drill_switch_ack_lost_then_continue(cluster: Cluster):
    with httpx.Client(timeout=10) as c:
        r = c.post(f"{cluster.node_urls[2]}/admin/drill", json={"armed": True})
        assert r.status_code == 200
        try:
            data = b"drill test payload" * 5
            r = submit(c, cluster, "drillobj", data, "drill-key", 0)
            # node2 persisted its shard but returned 503; two other nodes suffice
            assert r.status_code == 200, r.text
            v1 = r.json()["version"]

            # replay after the coordinator lost nothing but node would return 503:
            # probing finds the intact slot, no duplicate version
            r = submit(c, cluster, "drillobj", data, "drill-key", 0)
            assert r.status_code == 200
            assert r.json()["version"] == v1
            assert get(c, cluster, "drillobj").content == data
        finally:
            c.post(f"{cluster.node_urls[2]}/admin/drill", json={"armed": False})


def test_drill_pending_restart_coordinator_continuation(cluster: Cluster):
    # arm two nodes: first submit reaches at most one durable ack -> 503
    with httpx.Client(timeout=10) as c:
        for i in (1, 2):
            assert c.post(f"{cluster.node_urls[i]}/admin/drill", json={"armed": True}).status_code == 200
        data = b"restart continuation payload!!"
        r = submit(c, cluster, "restartobj", data, "rc-key", 0)
        assert r.status_code == 503

    cluster.restart_coordinator()

    with httpx.Client(timeout=10) as c:
        # coordinator restarts, probes existing shards (node0 persisted), and
        # continues the same pending write -> single published version
        r = submit(c, cluster, "restartobj", data, "rc-key", 0)
        assert r.status_code == 200, r.text
        assert r.json()["version"] == 1
        assert get(c, cluster, "restartobj").content == data
        for i in (1, 2):
            c.post(f"{cluster.node_urls[i]}/admin/drill", json={"armed": False})


# ---------------------------------------------------------------------------
# repair
# ---------------------------------------------------------------------------

def test_repair_needs_two_trusted_shards(cluster: Cluster):
    data = bytes((j * 13) % 256 for j in range(300))
    with httpx.Client(timeout=10) as c:
        r = submit(c, cluster, "repairtwo", data, "r2-key", 0)
        assert r.status_code == 200
        write_id = r.json()["write_id"]

        c.post(f"{cluster.node_urls[0]}/admin/shards/{write_id}/0/corrupt")
        c.post(f"{cluster.node_urls[2]}/admin/shards/{write_id}/2/corrupt")

        r = get(c, cluster, "repairtwo")
        assert r.status_code == 503
        r = c.post(f"{cluster.coord_url}/objects/repairtwo/repair")
        assert r.status_code == 503
        assert set(r.json()["detail"]["defective"]) == {0, 2}


def test_repair_single_corrupt_shard(cluster: Cluster):
    data = bytes((j * 13) % 256 for j in range(300))
    with httpx.Client(timeout=10) as c:
        r = submit(c, cluster, "repairone", data, "r1-key", 0)
        assert r.status_code == 200
        write_id = r.json()["write_id"]

        r = c.post(f"{cluster.node_urls[0]}/admin/shards/{write_id}/0/corrupt")
        assert r.status_code == 200

        # reads via the corrupt node must be rejected; two healthy shards
        # still serve exact bytes
        r = get(c, cluster, "repairone")
        assert r.status_code == 200
        assert r.content == data

        r = c.post(f"{cluster.coord_url}/objects/repairone/repair")
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "repaired"
        assert body["repaired_shards"] == [0]

        # second repair is a no-op
        r = c.post(f"{cluster.coord_url}/objects/repairone/repair")
        assert r.status_code == 200
        assert r.json()["status"] == "healthy"

        r = get(c, cluster, "repairone")
        assert r.status_code == 200
        assert r.content == data


def test_repair_does_not_overwrite_new_version(cluster: Cluster):
    with httpx.Client(timeout=10) as c:
        data1 = b"v1 payload for repair race"
        r = submit(c, cluster, "repairrace", data1, "rr1", 0)
        assert r.status_code == 200
        w1 = r.json()["write_id"]

        # stop node0, overwrite to v2 while it is away: node0 keeps its v1
        # shards but has none of the v2 shards
        cluster.stop_node(0)
        data2 = b"v2 payload for repair race!!"
        r = submit(c, cluster, "repairrace", data2, "rr2", 1)
        assert r.status_code == 200
        assert r.json()["version"] == 2
        cluster.start_node(0)

        # repair current version (2) fills node0's missing v2 shard-0 slot;
        # the old v1 shard that node0 already holds must never be touched
        r = c.post(f"{cluster.coord_url}/objects/repairrace/repair")
        assert r.status_code == 200, r.text
        assert r.json()["version"] == 2
        assert r.json()["repaired_shards"] == [0]
        assert get(c, cluster, "repairrace").content == data2

        # node0's v1 shard-0 is still the original intact one (each node holds
        # exactly one shard of every write) ...
        listing = c.get(f"{cluster.node_urls[0]}/writes/{w1}")
        assert listing.status_code == 200
        assert {s["index"]: s["intact"] for s in listing.json()["shards"]} == {
            0: True,
        }
        # ... and the pinned old version is still byte-exact
        r = get(c, cluster, "repairrace", version=1)
        assert r.status_code == 200
        assert r.content == data1

        # a second repair is a no-op now that every v2 slot is intact
        r = c.post(f"{cluster.coord_url}/objects/repairrace/repair")
        assert r.status_code == 200
        assert r.json()["status"] == "healthy"


def test_replay_published_after_coordinator_restart(cluster: Cluster):
    data = b"published then coordinator bounced"
    with httpx.Client(timeout=10) as c:
        r = submit(c, cluster, "bounce", data, "bounce-key", 0)
        assert r.status_code == 200
        assert r.json()["version"] == 1
    cluster.restart_coordinator()
    with httpx.Client(timeout=10) as c:
        r = submit(c, cluster, "bounce", data, "bounce-key", 0)
        assert r.status_code == 200
        assert r.json()["version"] == 1  # no second version
        assert r.json()["idempotent_replay"] is True
        assert get(c, cluster, "bounce").content == data


def test_get_pinned_version_survives_concurrent_overwrite(cluster: Cluster):
    import threading

    with httpx.Client(timeout=10) as c:
        assert submit(c, cluster, "pin", b"v1" * 20, "pin1", 0).status_code == 200
        assert submit(c, cluster, "pin", b"v2" * 20, "pin2", 1).status_code == 200
        assert submit(c, cluster, "pin", b"v3" * 20, "pin3", 2).status_code == 200

    results = {}

    def read_version(v):
        with httpx.Client(timeout=10) as c:
            results[v] = get(c, cluster, "pin", version=v).content

    threads = [threading.Thread(target=read_version, args=(v,)) for v in (1, 2, 3)]
    for t in threads:
        t.start()
    # concurrently overwrite again
    with httpx.Client(timeout=10) as c:
        submit(c, cluster, "pin", b"v4" * 20, "pin4", 3)
    for t in threads:
        t.join()
    assert results == {1: b"v1" * 20, 2: b"v2" * 20, 3: b"v3" * 20}


def test_corrupt_node_slot_rejects_plain_overwrite(cluster: Cluster):
    data = b"immutable slot content"
    with httpx.Client(timeout=10) as c:
        r = submit(c, cluster, "imm", data, "imm-key", 0)
        assert r.status_code == 200
        write_id = r.json()["write_id"]
        c.post(f"{cluster.node_urls[1]}/admin/shards/{write_id}/1/corrupt")

        # a plain PUT onto the corrupt slot is refused (409); only the repair
        # path may rewrite it, and only with bytes matching the published digest
        meta = c.get(f"{cluster.node_urls[1]}/shards/{write_id}/1/meta").json()
        r = c.put(
            f"{cluster.node_urls[1]}/shards/{write_id}/1",
            content=b"x" * meta["length"],
            headers={"X-Shard-SHA256": sha(b"x" * meta["length"])},
        )
        assert r.status_code == 409

        # repair via coordinator restores it using the published digest
        r = c.post(f"{cluster.coord_url}/objects/imm/repair")
        assert r.status_code == 200
        assert r.json()["repaired_shards"] == [1]
        assert get(c, cluster, "imm").content == data


# ---------------------------------------------------------------------------
# validation / not found
# ---------------------------------------------------------------------------

def test_validation_errors(cluster: Cluster):
    with httpx.Client(timeout=10) as c:
        # missing headers -> 422
        r = c.put(f"{cluster.coord_url}/objects/o", content=b"x")
        assert r.status_code == 422

        # empty body -> 422
        r = c.put(
            f"{cluster.coord_url}/objects/o", content=b"",
            headers={"X-Request-Key": "k", "X-Expected-Version": "0"},
        )
        assert r.status_code == 422

        # oversized body -> 422
        r = c.put(
            f"{cluster.coord_url}/objects/o", content=b"x" * 65537,
            headers={"X-Request-Key": "k", "X-Expected-Version": "0"},
        )
        assert r.status_code == 422

        # invalid key characters -> 422
        r = c.put(
            f"{cluster.coord_url}/objects/o", content=b"x",
            headers={"X-Request-Key": "bad key!", "X-Expected-Version": "0"},
        )
        assert r.status_code == 422

        # invalid name (contains a space) -> 422
        r = c.put(
            f"{cluster.coord_url}/objects/bad%20name", content=b"x",
            headers={"X-Request-Key": "k", "X-Expected-Version": "0"},
        )
        assert r.status_code == 422

        # negative expected version -> 422
        r = c.put(
            f"{cluster.coord_url}/objects/o", content=b"x",
            headers={"X-Request-Key": "k", "X-Expected-Version": "-1"},
        )
        assert r.status_code == 422

        # node-side bad shard digest -> 422
        r = c.put(
            f"{cluster.node_urls[0]}/shards/w1_v9/0",
            content=b"abc",
            headers={"X-Shard-SHA256": "z" * 64},
        )
        assert r.status_code == 422


def test_not_found(cluster: Cluster):
    with httpx.Client(timeout=10) as c:
        assert get(c, cluster, "never-existed").status_code == 404
        assert submit(c, cluster, "exists", b"hi", "e", 0).status_code == 200
        assert get(c, cluster, "exists", version=99).status_code == 404
        assert c.post(f"{cluster.coord_url}/objects/never/repair").status_code == 404
