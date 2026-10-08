"""Redis-backed buffers for outbound batch mode.

One list per (integration, output type). Writers RPUSH records stamped with
their enqueue time; a flush, under a per-buffer lock, reads up to a batch
from the head, hands it to a send callback, and trims it only once the
callback returns. A send that raises leaves the records for the next flush,
so delivery is at-least-once: a crash between a 2xx and the trim re-sends.

A buffer is capped (oldest records dropped past the cap) and backs off after
a transient failure, so an endpoint outage neither grows Redis without bound
nor gets hit once per incoming record.
"""
import json
import logging
import math
import time
import uuid
from typing import Any, Awaitable, Callable, List, NamedTuple, Optional

import redis.asyncio as redis

from app import settings

logger = logging.getLogger(__name__)

KEY_PREFIX = "outbound_buffer"

# Delete the lock only if this flusher still holds it.
_RELEASE_LOCK = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
end
return 0
"""

# Trim only while still holding the lock. A flusher whose lock expired
# mid-send may have had the same head read and trimmed by the next one;
# trimming again would drop records nobody sent.
_TRIM_IF_LOCKED = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    redis.call('ltrim', KEYS[2], ARGV[2], -1)
    return 1
end
return 0
"""


def buffer_key(integration_id, output_type) -> str:
    return f"{KEY_PREFIX}.{integration_id}.{output_type}"


def _lock_key(integration_id, output_type) -> str:
    return f"{buffer_key(integration_id, output_type)}.lock"


def _backoff_key(integration_id, output_type) -> str:
    return f"{buffer_key(integration_id, output_type)}.backoff"


def _failures_key(integration_id, output_type) -> str:
    return f"{buffer_key(integration_id, output_type)}.failures"


def _dropped_key(integration_id, output_type) -> str:
    return f"{buffer_key(integration_id, output_type)}.dropped"


# The cap trim holds the flush lock for two Redis calls; a crash in between must
# not block flushes for the full OUTBOUND_FLUSH_LOCK_SECONDS.
CAP_TRIM_LOCK_SECONDS = 10

# Upper bound on an endpoint's Retry-After, so a bogus value cannot park a buffer for weeks.
MAX_RETRY_AFTER_SECONDS = 24 * 60 * 60


class PushResult(NamedTuple):
    length: int
    dropped: int = 0


class FlushResult(NamedTuple):
    locked_out: bool = False
    batches_sent: int = 0
    records_sent: int = 0


class OutboundBuffer:

    def __init__(self, db_client: Optional[redis.Redis] = None):
        self.db_client = db_client or redis.Redis(
            host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=settings.REDIS_STATE_DB,
        )

    async def _acquire(self, lock_key: str, lock_seconds: int) -> Optional[str]:
        token = uuid.uuid4().hex
        return token if await self.db_client.set(lock_key, token, nx=True, ex=lock_seconds) else None

    async def _release(self, lock_key: str, token: str):
        try:
            await self.db_client.eval(_RELEASE_LOCK, 1, lock_key, token)
        except Exception as e:
            # The lock expires on its own; a stuck release only delays the next flush.
            logger.warning(f"Could not release '{lock_key}': {type(e).__name__}: {e}")

    async def push(self, integration_id, output_type, record: Any, max_records: Optional[int] = None) -> PushResult:
        """Append a JSON-safe record. Past max_records the oldest records are
        dropped; PushResult.dropped says how many."""
        max_records = max_records or settings.OUTBOUND_BUFFER_MAX_RECORDS
        key = buffer_key(integration_id, output_type)
        entry = json.dumps({"enqueued_at": time.time(), "record": record})
        length = await self.db_client.rpush(key, entry)
        if length <= max_records:
            return PushResult(length=length)
        # Trimmed under the flush lock: a flush trims the head by the count it
        # read, so trimming the head under it would drop records it never sent.
        # If a flush holds the lock, the next push trims instead.
        lock_key = _lock_key(integration_id, output_type)
        token = await self._acquire(lock_key, CAP_TRIM_LOCK_SECONDS)
        if not token:
            return PushResult(length=length)
        try:
            excess = await self.db_client.llen(key) - max_records
            if excess > 0 and await self.db_client.eval(_TRIM_IF_LOCKED, 2, lock_key, key, token, excess):
                return PushResult(length=length - excess, dropped=excess)
            return PushResult(length=length)
        finally:
            await self._release(lock_key, token)

    async def backoff_remaining(self, integration_id, output_type) -> float:
        """Seconds until the buffer may be flushed again after a transient failure."""
        until = await self.db_client.get(_backoff_key(integration_id, output_type))
        try:
            return max(0.0, float(until) - time.time()) if until else 0.0
        except (TypeError, ValueError):
            return 0.0

    async def start_backoff(self, integration_id, output_type, retry_after: Optional[float] = None) -> float:
        """Hold flushes after a transient failure; returns the delay.

        Retry-After when the endpoint gave one, otherwise exponential in the
        number of consecutive failures, capped at OUTBOUND_BACKOFF_MAX_SECONDS.
        """
        failures_key = _failures_key(integration_id, output_type)
        failures = await self.db_client.incr(failures_key)
        # Forget the streak once the endpoint has been left alone for a while.
        await self.db_client.expire(failures_key, settings.OUTBOUND_BACKOFF_MAX_SECONDS * 4)
        if retry_after is not None:
            delay = min(max(float(retry_after), 1.0), MAX_RETRY_AFTER_SECONDS)
        else:
            exponent = min(int(failures) - 1, 20)
            delay = min(settings.OUTBOUND_BACKOFF_INITIAL_SECONDS * 2 ** exponent, settings.OUTBOUND_BACKOFF_MAX_SECONDS)
        await self.db_client.setex(
            _backoff_key(integration_id, output_type), math.ceil(delay), str(time.time() + delay),
        )
        return delay

    async def clear_backoff(self, integration_id, output_type):
        await self.db_client.delete(
            _backoff_key(integration_id, output_type), _failures_key(integration_id, output_type),
        )

    async def add_dropped(self, integration_id, output_type, count: int):
        """Count records dropped by the cap until take_dropped reports them."""
        await self.db_client.incrby(_dropped_key(integration_id, output_type), count)

    async def take_dropped(self, integration_id, output_type) -> int:
        """Records dropped since the last call."""
        key = _dropped_key(integration_id, output_type)
        count = int(await self.db_client.get(key) or 0)
        if count:
            # Subtract rather than delete: drops counted since the GET stay for the next report.
            await self.db_client.decrby(key, count)
        return count

    async def length(self, integration_id, output_type) -> int:
        return await self.db_client.llen(buffer_key(integration_id, output_type))

    async def _is_due(self, key: str, max_batch_size: int, max_wait_seconds: float) -> bool:
        length = await self.db_client.llen(key)
        if not length:
            return False
        if length >= max_batch_size:
            return True
        head = await self.db_client.lindex(key, 0)
        if head is None:
            return False
        try:
            enqueued_at = json.loads(head)["enqueued_at"]
        except (ValueError, KeyError, TypeError):
            return True  # an unreadable head must not block the buffer forever
        return time.time() - enqueued_at >= max_wait_seconds

    async def flush(
            self,
            integration_id,
            output_type,
            *,
            max_batch_size: int,
            max_wait_seconds: float,
            send: Callable[[List[Any]], Awaitable[None]],
            lock_seconds: Optional[int] = None,
            request_timeout: Optional[float] = None,
    ) -> FlushResult:
        """Send batches while the buffer is due: holding a full batch, or a head
        older than max_wait_seconds. Returns without sending if another flush
        holds the lock. Exceptions from `send` propagate after the lock is released."""
        lock_seconds = lock_seconds or settings.OUTBOUND_FLUSH_LOCK_SECONDS
        request_timeout = request_timeout or settings.OUTBOUND_REQUEST_TIMEOUT_SECONDS
        key = buffer_key(integration_id, output_type)
        lock_key = _lock_key(integration_id, output_type)
        token = await self._acquire(lock_key, lock_seconds)
        if not token:
            return FlushResult(locked_out=True)
        # Stop starting sends once one more might outlive the lock.
        deadline = time.monotonic() + lock_seconds - 2 * request_timeout
        batches_sent = records_sent = 0
        try:
            while time.monotonic() < deadline and await self._is_due(key, max_batch_size, max_wait_seconds):
                entries = await self.db_client.lrange(key, 0, max_batch_size - 1)
                if not entries:
                    break
                records = []
                for entry in entries:
                    try:
                        records.append(json.loads(entry)["record"])
                    except (ValueError, KeyError, TypeError):
                        logger.warning(f"Dropping an unreadable record from outbound buffer '{key}'.")
                if records:
                    await send(records)
                if not await self.db_client.eval(_TRIM_IF_LOCKED, 2, lock_key, key, token, len(entries)):
                    logger.warning(f"Lost the flush lock for '{key}' while sending; stopping this flush.")
                    break
                batches_sent += 1
                records_sent += len(records)
        finally:
            await self._release(lock_key, token)
        return FlushResult(batches_sent=batches_sent, records_sent=records_sent)
