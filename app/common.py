"""共享工具：2-of-3 XOR 纠删编码、SHA-256、输入校验。

编码方案：
  - 原长 L（1..65536），半长 half = ceil(L/2)
  - 片 0 = 前半片（长 half）
  - 片 1 = 后半片，右侧补零至 half
  - 片 2 = 片0 与 片1 的逐字节异或（长 half）
任意两片即可还原准确字节（按原长截断）。
"""
from __future__ import annotations

import hashlib
import re

MIN_OBJECT_SIZE = 1
MAX_OBJECT_SIZE = 65536
SHARD_COUNT = 3

# 对象名限制：避免路径注入，同时便于节点落盘
NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
REQUEST_KEY_MAX_LEN = 128
SHA256_HEX_LEN = 64


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def valid_name(name: str) -> bool:
    return bool(NAME_RE.match(name))


def split_shards(data: bytes) -> list[bytes]:
    """把原文拆成三片等长分片：[前半, 补零后半, 异或片]。"""
    half = (len(data) + 1) // 2
    front = data[:half]
    back = data[half:].ljust(half, b"\x00")
    xor = bytes(a ^ b for a, b in zip(front, back))
    return [front, back, xor]


def _xor_bytes(a: bytes, b: bytes) -> bytes:
    return bytes(x ^ y for x, y in zip(a, b))


def reconstruct(shards: dict[int, bytes], size: int) -> bytes:
    """用任意两片（或更多）还原原始字节，按原长 size 截断。"""
    front = shards.get(0)
    back = shards.get(1)
    xor = shards.get(2)
    if front is None:
        front = _xor_bytes(back, xor)
    if back is None:
        back = _xor_bytes(front, xor)
    return (front + back)[:size]


def derive_shard(shards: dict[int, bytes], want: int) -> bytes:
    """由已知两片推导出第 want 片（用于修复缺损片）。"""
    front = shards.get(0)
    back = shards.get(1)
    xor = shards.get(2)
    if front is None:
        front = _xor_bytes(back, xor)
    if back is None:
        back = _xor_bytes(front, xor)
    if xor is None:
        xor = _xor_bytes(front, back)
    return {0: front, 1: back, 2: xor}[want]
