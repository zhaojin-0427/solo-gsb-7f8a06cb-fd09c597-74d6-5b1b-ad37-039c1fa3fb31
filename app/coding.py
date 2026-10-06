"""2+1 erasure coding.

The original object is split into two equal-length halves:

* shard 0 - front half
* shard 1 - back half, zero-padded when the object has odd length
* shard 2 - parity, byte-wise XOR of shard 0 and shard 1

Any two shards reconstruct the exact original bytes; shard 2 together with
either shard 0 or shard 1 also reveals the missing one. All three shards have
the same length, so padding bytes are stripped using the recorded original
length.
"""

MIN_OBJECT_SIZE = 1
MAX_OBJECT_SIZE = 65_536

SHARD_FRONT = 0
SHARD_BACK = 1
SHARD_PARITY = 2
SHARD_INDICES = (SHARD_FRONT, SHARD_BACK, SHARD_PARITY)


class CodingError(ValueError):
    """Raised when shards are malformed or fail checksum reconstruction."""


def validate_object(data: bytes) -> None:
    if not isinstance(data, (bytes, bytearray)):
        raise CodingError("object body must be raw bytes")
    if not (MIN_OBJECT_SIZE <= len(data) <= MAX_OBJECT_SIZE):
        raise CodingError(
            f"object size must be between {MIN_OBJECT_SIZE} and {MAX_OBJECT_SIZE} bytes, "
            f"got {len(data)}"
        )


def shard_len(orig_len: int) -> int:
    return (orig_len + 1) // 2


def split(data: bytes) -> list[bytes]:
    """Split an object into [front, back-padded, parity]."""
    validate_object(data)
    half = shard_len(len(data))
    front = data[:half]
    back = data[half:]
    if len(back) < half:
        back = back + b"\x00" * (half - len(back))
    parity = bytes(a ^ b for a, b in zip(front, back))
    return [front, back, parity]


def reconstruct(shards: dict[int, bytes], orig_len: int) -> bytes:
    """Reconstruct the object from any two trusted shards.

    `shards` maps shard index (0/1/2) to shard bytes. Raises CodingError if
    fewer than two shards are supplied, the shard lengths disagree with the
    recorded original length, or the parity relation does not hold.
    """
    half = shard_len(orig_len)
    present = {}
    for index, payload in shards.items():
        if index not in SHARD_INDICES:
            raise CodingError(f"unknown shard index {index}")
        if not isinstance(payload, (bytes, bytearray)) or len(payload) != half:
            raise CodingError(
                f"shard {index} must be exactly {half} bytes, got "
                f"{len(payload) if isinstance(payload, (bytes, bytearray)) else 'non-bytes'}"
            )
        present[index] = bytes(payload)

    if len(present) < 2:
        raise CodingError("at least two shards are required for reconstruction")

    s = [present.get(i) for i in SHARD_INDICES]
    if s[0] is None:
        s[0] = bytes(a ^ b for a, b in zip(s[1], s[2]))
    elif s[1] is None:
        s[1] = bytes(a ^ b for a, b in zip(s[0], s[2]))
    elif s[2] is None:
        s[2] = bytes(a ^ b for a, b in zip(s[0], s[1]))

    # Always verify the parity relation, even when all three shards were given.
    parity_check = bytes(a ^ b for a, b in zip(s[0], s[1]))
    if parity_check != s[2]:
        raise CodingError("parity mismatch: shards do not belong to the same write")

    data = (s[0] + s[1])[:orig_len]
    if len(data) != orig_len:
        raise CodingError("reconstructed object length disagrees with metadata")
    return data
