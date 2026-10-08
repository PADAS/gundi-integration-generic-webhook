"""Per-bundle redelivery-dedup progress records.

Ported from gundi-dispatcher-er (core/batch_progress.py) rather than shared:
the dispatcher keeps it in its own tree and its cache access is synchronous.
One Redis key per (bundle, destination, output type) holds a fingerprint of
the request plan plus a bitmap of the requests already answered with 2xx, so
a redelivered bundle resumes after the last delivered request.

Bits index requests (chunks), not items: a generic endpoint answers a batch
request with one status and no per-item result. That makes the chunking part
of what a bit means, so the fingerprint binds the chunk size as well as the
ordered item ids, and a config change between deliveries invalidates the
record instead of mapping old bits onto different chunks.
"""
import hashlib
import logging
from typing import Iterable, Optional, Set

import redis.asyncio as redis

from app import settings

logger = logging.getLogger(__name__)

KEY_PREFIX = "outbound_batch_progress"
FINGERPRINT_BYTES = 8


def progress_key(batch_id, integration_id, output_type) -> str:
    return f"{KEY_PREFIX}.{batch_id}.{integration_id}.{output_type}"


def fingerprint(item_ids: Iterable, chunk_size: int) -> bytes:
    """8-byte digest binding a record to an exact ordered id list and chunk size.

    Length-prefixed with fixed-width lengths rather than delimiter-joined:
    gundi_id may be any string, and a delimiter join lets two different lists
    collide (["a|b", "c"] vs ["a", "b|c"]). A collision would make decode()
    report requests as delivered that never were, the one outcome this must
    not produce. Truncating SHA-256 leaves ~2^-64 per comparison, acceptable
    because a record is only compared against plans for its own key.
    """
    ids = [str(item_id).encode() for item_id in item_ids]
    h = hashlib.sha256()
    h.update(int(chunk_size).to_bytes(4, "big"))
    h.update(len(ids).to_bytes(4, "big"))
    for raw in ids:
        h.update(len(raw).to_bytes(4, "big"))
        h.update(raw)
    return h.digest()[:FINGERPRINT_BYTES]


def encode(fp: bytes, delivered: Set[int], n: int) -> bytes:
    """fingerprint || bitmap, where bit i is set when request i was delivered."""
    bitmap = bytearray((n + 7) // 8)
    for index in delivered:
        if 0 <= index < n:
            bitmap[index // 8] |= 1 << (index % 8)
    return bytes(fp) + bytes(bitmap)


def decode(raw, expected_fingerprint: bytes, n: int) -> Set[int]:
    """Delivered request indices, or an empty set when the record is unusable.

    Empty means "nothing known to be delivered": a missing or truncated record,
    or one written for a different plan. Callers then send everything; a
    duplicate is acceptable, a silently skipped request is not.
    """
    try:
        if len(expected_fingerprint) != FINGERPRINT_BYTES:
            return set()
        if not raw or len(raw) < FINGERPRINT_BYTES:
            return set()
        if bytes(raw[:FINGERPRINT_BYTES]) != bytes(expected_fingerprint):
            return set()
        bitmap = raw[FINGERPRINT_BYTES:]
        delivered = set()
        for index in range(n):
            byte_index = index // 8
            if byte_index >= len(bitmap):
                break
            if bitmap[byte_index] & (1 << (index % 8)):
                delivered.add(index)
        return delivered
    except (TypeError, ValueError) as e:
        logger.warning(f"Discarding unusable batch progress record: {type(e).__name__} {e}")
        return set()


class BatchProgressStore:
    """Reads and writes progress records. Never raises: a Redis failure reads
    as "nothing delivered" and skips the write, costing duplicates at worst."""

    def __init__(self, db_client: Optional[redis.Redis] = None):
        self.db_client = db_client or redis.Redis(
            host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=settings.REDIS_STATE_DB,
        )

    async def read(self, batch_id, integration_id, output_type) -> Optional[bytes]:
        try:
            return await self.db_client.get(progress_key(batch_id, integration_id, output_type))
        except Exception as e:
            logger.warning(f"Error reading batch progress: {type(e).__name__}: {e}")
            return None

    async def write(self, batch_id, integration_id, output_type, fp: bytes, delivered: Set[int], n: int):
        if not delivered:
            return
        try:
            await self.db_client.setex(
                progress_key(batch_id, integration_id, output_type),
                settings.OUTBOUND_BATCH_PROGRESS_TTL_SECONDS,
                encode(fp, delivered, n),
            )
        except Exception as e:
            logger.warning(f"Error writing batch progress: {type(e).__name__}: {e}")
