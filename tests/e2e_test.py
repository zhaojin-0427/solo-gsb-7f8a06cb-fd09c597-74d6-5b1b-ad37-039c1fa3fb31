#!/usr/bin/env python3
"""端到端场景测试（针对本地进程部署，非 Docker）。

覆盖：基本读写、边界长度、幂等重试、版本 CAS、非法输入、
单/双节点故障、协调端重启续交、损坏片修复、节点不可变语义、并发覆盖。

用法：
  1. 启动 3 个节点（8001/8002/8003）与协调端（8000），见 README「本地开发」。
  2. NODE_DIRS 指向三个节点的数据目录（用于注入损坏/删除）。
  3. python3 tests/e2e_test.py
"""
import hashlib
import os
import random
import subprocess
import sys
import time

import httpx

BASE = os.environ.get("BASE", "http://127.0.0.1:8000")
NODES = os.environ.get(
    "TEST_NODES", "http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003"
).split(",")
DIRS = os.environ.get(
    "NODE_DIRS", "/tmp/shardtest/n1,/tmp/shardtest/n2,/tmp/shardtest/n3"
).split(",")
COORD_PID = os.environ.get("COORD_PID_FILE", "/tmp/shardtest/coord.pid")
COORD_LOG = os.environ.get("COORD_LOG", "/tmp/shardtest/coord.log")
COORD_DB = os.environ.get("COORD_DB", "/tmp/shardtest/coord/coordinator.db")
UVICORN = os.environ.get("UVICORN", "uvicorn")
APP_DIR = os.environ.get("APP_DIR", os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

FAILS = []


def check(cond, label):
    print(("PASS " if cond else "FAIL ") + label)
    if not cond:
        FAILS.append(label)


def put(name, key, expected, body, **kw):
    return httpx.put(
        f"{BASE}/objects/{name}",
        content=body,
        headers={"X-Request-Key": key, "X-Expected-Version": str(expected)},
        timeout=10,
        **kw,
    )


def get(name):
    return httpx.get(f"{BASE}/objects/{name}", timeout=10)


def drill(node, on):
    return httpx.post(f"{node}/admin/drill", json={"fail_after_first_store": on}, timeout=5)


def shard_file(node_dir, name, version, idx):
    return os.path.join(node_dir, "shards", name, str(version), f"{idx}.shard")


# ---------- 1. 基本提交 / 读取 ----------
body = b"hello erasure world" * 3  # 奇数长度
r = put("obj1", "k1", 0, body)
check(r.status_code == 201 and r.json()["version"] == 1, f"put new object -> 201 v1 ({r.status_code} {r.text})")
r = get("obj1")
check(r.status_code == 200 and r.content == body, "get returns exact bytes")
check(r.headers.get("x-object-version") == "1", "get version header == 1")

# 1 字节与 65536 字节边界
b1 = b"x"
r = put("tiny", "k1", 0, b1)
check(r.status_code == 201, "put 1-byte object")
check(get("tiny").content == b1, "get 1-byte object exact")
big = random.randbytes(65536)
r = put("big", "k1", 0, big)
check(r.status_code == 201, "put 65536-byte object")
check(get("big").content == big, "get 65536-byte object exact")

# ---------- 2. 幂等 ----------
r = put("obj1", "k1", 0, body)  # 同名同键同内容重试（期望版本照旧填 0）
check(r.status_code == 200 and r.json()["version"] == 1 and r.json()["replayed"], f"replay same key -> original version ({r.status_code} {r.text})")
r = put("obj1", "k1", 0, b"different content")
check(r.status_code == 409, f"same key different content -> 409 ({r.status_code})")
r = put("obj1", "k2", 0, body)  # 过期期望版本
check(r.status_code == 409, f"stale expected version -> 409 ({r.status_code})")
r = put("obj1", "k2", 1, b"v2-content")
check(r.status_code == 201 and r.json()["version"] == 2, "put v2 with expected=1")
check(get("obj1").content == b"v2-content", "get returns v2 bytes")

# ---------- 3. 非法输入 422 / 不存在 404 ----------
check(put("obj2", "k", 0, b"").status_code == 422, "empty body -> 422")
check(put("obj2", "k", 0, random.randbytes(65537)).status_code == 422, "65537 bytes -> 422")
check(put("obj2", "", 0, b"a").status_code == 422, "missing request key -> 422")
check(put("obj2", "k", -1, b"a").status_code == 422, "negative expected -> 422")
check(httpx.put(f"{BASE}/objects/obj2", content=b"a", headers={"X-Request-Key": "k"}).status_code == 422, "missing expected header -> 422")
check(put("bad/name", "k", 0, b"a").status_code in (404, 405, 422), "slash in name rejected")
check(put("bad name", "k", 0, b"a").status_code == 422, "space in name -> 422")
check(get("nosuch").status_code == 404, "get missing -> 404")
check(httpx.get(f"{BASE}/objects/nosuch/meta").status_code == 404, "meta missing -> 404")
check(httpx.post(f"{BASE}/objects/nosuch/repair").status_code == 404, "repair missing -> 404")

# ---------- 4. 演练开关：单节点离线仍可提交和读取 ----------
check(drill(NODES[2], True).status_code == 200, "enable drill on node3")
r = put("obj1", "k3", 2, b"v3-content")
check(r.status_code == 201 and r.json()["version"] == 3, f"put with node3 failing -> 201 v3 ({r.status_code} {r.text})")
r = get("obj1")
check(r.status_code == 200 and r.content == b"v3-content", "get with node3 failing")
r = httpx.get(f"{BASE}/objects/obj1/meta")
check(r.status_code == 200 and r.json()["version"] == 3, "meta shows v3")

# ---------- 5. 双节点离线：提交 503 且不改已发布版本；读取 503 ----------
check(drill(NODES[1], True).status_code == 200, "enable drill on node2")
r = put("obj1", "k4", 3, b"v4-content")
check(r.status_code == 503, f"put with 2 nodes failing -> 503 ({r.status_code})")
r = httpx.get(f"{BASE}/objects/obj1/meta")
check(r.json()["version"] == 3, "published version unchanged after 503 put")
r = get("obj1")
check(r.status_code == 503, f"get with only 1 shard -> 503 ({r.status_code})")

# ---------- 6. 协调端重启后续交：不产生第二版本 ----------
# k4 处于 pending(v4)。重启协调端，恢复 node3，重试同键同内容 -> 应发布 v4 而非 v5
print(".. restarting coordinator")
with open(COORD_PID) as f:
    os.kill(int(f.read().strip()), 15)
time.sleep(1.5)
env = dict(os.environ)
env.update(DB_PATH=COORD_DB, NODES=",".join(NODES))
proc = subprocess.Popen(
    [UVICORN, "app.coordinator:app", "--port", "8000", "--host", "127.0.0.1"],
    cwd=APP_DIR, env=env,
    stdout=open(COORD_LOG, "ab"), stderr=subprocess.STDOUT)
with open(COORD_PID, "w") as f:
    f.write(str(proc.pid))
for _ in range(50):
    try:
        if httpx.get(f"{BASE}/healthz", timeout=1).status_code == 200:
            break
    except Exception:
        time.sleep(0.2)
check(drill(NODES[2], False).status_code == 200, "disable drill on node3 (node2 still failing)")
r = put("obj1", "k4", 3, b"v4-content")  # 同键续交
check(r.status_code == 201 and r.json()["version"] == 4, f"resume after coordinator restart publishes SAME version v4 ({r.status_code} {r.text})")
r = httpx.get(f"{BASE}/objects/obj1/meta")
check(r.json()["version"] == 4, "no second version created")
check(get("obj1").content == b"v4-content", "get v4 bytes (node2 still down)")

# 重试已发布的 k4 -> 原版本
r = put("obj1", "k4", 3, b"v4-content")
check(r.status_code == 200 and r.json()["version"] == 4 and r.json()["replayed"], "retry published key -> original version")

# ---------- 7. 修复：补齐缺失片 ----------
# node2 仍离线（drill），node2 上 v4 的片缺失。关闭 drill 后修复。
check(drill(NODES[1], False).status_code == 200, "disable drill on node2")
r = httpx.head(f"{NODES[1]}/shards/obj1/4/1")
check(r.status_code == 404, "node2 missing v4 shard before repair")
r = httpx.post(f"{BASE}/objects/obj1/repair")
check(r.status_code == 200 and len(r.json()["repaired"]) == 1, f"repair fills missing shard ({r.status_code} {r.text})")
r = httpx.head(f"{NODES[1]}/shards/obj1/4/1")
check(r.status_code == 200, "node2 has v4 shard after repair")

# ---------- 8. 损坏片：读取仍可用；双片损坏 -> 503；按已发布摘要修复 ----------
p0 = shard_file(DIRS[0], "obj1", 4, 0)
orig0 = open(p0, "rb").read()
with open(p0, "r+b") as f:  # 就地翻字节，模拟落盘后损坏
    f.write(bytes([orig0[0] ^ 0xFF]))
check(get("obj1").content == b"v4-content", "get survives 1 corrupted shard")
p1 = shard_file(DIRS[1], "obj1", 4, 1)
orig1 = open(p1, "rb").read()
with open(p1, "r+b") as f:
    f.write(bytes([orig1[0] ^ 0xFF]))
check(get("obj1").status_code == 503, "get with 2 corrupted shards -> 503")
r = httpx.post(f"{BASE}/objects/obj1/repair")
check(r.status_code == 503, "repair with only 1 intact shard -> 503")
# 恢复片1，再修复片0
with open(p1, "wb") as f:
    f.write(orig1)
r = httpx.post(f"{BASE}/objects/obj1/repair")
check(r.status_code == 200 and any(x["shard_index"] == 0 for x in r.json()["repaired"]), f"repair heals corrupted shard0 ({r.text})")
with open(p0, "rb") as f:
    check(f.read() == orig0, "shard0 bytes restored to published digest")
check(get("obj1").content == b"v4-content", "get ok after repair")

# 节点不可变：异摘要同路径 -> 409
r = httpx.put(f"{NODES[0]}/shards/obj1/4/0", content=b"forged", headers={"X-Shard-Sha256": hashlib.sha256(b"forged").hexdigest()})
check(r.status_code == 409, "node rejects different digest for same write id (immutable)")
# 同摘要重复写 -> 幂等 200
sha0 = hashlib.sha256(orig0).hexdigest()
r = httpx.put(f"{NODES[0]}/shards/obj1/4/0", content=orig0, headers={"X-Shard-Sha256": sha0})
check(r.status_code == 200, "node idempotent rewrite same digest")

# ---------- 9. 并发覆盖不混片 ----------
import concurrent.futures as cf

payload_a = b"A" * 60000
payload_b = b"B" * 60000
r = put("race", "rk0", 0, payload_a)
check(r.status_code == 201, "race object v1")

# 顺序推到 v5 后并发双写同一期望版本
ver = 1
for i in range(1, 5):
    r = put("race", f"rk{i}", ver, bytes([65 + i]) * 60000)
    ver = r.json()["version"]
with cf.ThreadPoolExecutor(4) as ex:
    futs = [ex.submit(put, "race", "raceA", ver, payload_a), ex.submit(put, "race", "raceB", ver, payload_b)]
    codes = sorted(f.result().status_code for f in futs)
check(codes == [201, 409], f"concurrent same-expected puts -> one 201 one 409 ({codes})")
r = get("race")
check(r.status_code == 200 and r.content in (payload_a, payload_b), "concurrent overwrite: get returns one whole version, no mixed shards")

# ---------- 10. 修复与覆盖并发：不用旧片覆盖新版 ----------
cur = httpx.get(f"{BASE}/objects/race/meta").json()["version"]
os.remove(shard_file(DIRS[0], "race", cur, 0))
r = httpx.post(f"{BASE}/objects/race/repair")
check(r.status_code == 200, "repair before overwrite ok")
newer = b"Z" * 40000
r = put("race", "raceC", cur, newer)
check(r.status_code == 201, "overwrite after repair")
check(get("race").content == newer, "new version bytes intact after repair+overwrite")

print()
if FAILS:
    print(f"{len(FAILS)} FAILURES:")
    for f_ in FAILS:
        print(" -", f_)
    sys.exit(1)
print("ALL TESTS PASSED")
