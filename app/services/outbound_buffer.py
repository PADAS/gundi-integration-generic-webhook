"""Redis-backed buffers for outbound batch mode.

One list per (integration, output type). Writers RPUSH records stamped with
their enqueue time; a flush, under a per-buffer lock, reads up to a batch
from the head, hands it to a send callback, and trims it only once the
callback returns. A send that raises leaves the records for the next flush,
so delivery is at-least-once: a crash between a 2xx and the trim re-sends.

A buffer is capped (oldest records dropped past the cap) and backs off after
a transient failure, so an endpoint outage neither grows Redis without bound
nor gets hit once per incoming record. Drops are counted in a `.dropped` key
for the caller to report (pending_dropped, then acknowledge_dropped).
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

# Append, then apply the cap unless a flush holds the lock: a flush trims the
# head by the count it read, so trimming the head under it would drop records
# it never sent. The flush applies the deferred cap when it releases (below).
# KEYS: buffer, lock, dropped counter. ARGV: entry, cap. Returns {length, dropped}.
_PUSH = """
local length = redis.call('rpush', KEYS[1], ARGV[1])
local excess = length - tonumber(ARGV[2])
if excess > 0 and redis.call('exists', KEYS[2]) == 0 then
    redis.call('ltrim', KEYS[1], excess, -1)
    redis.call('incrby', KEYS[3], excess)
    return {length - excess, excess}
end
return {length, 0}
"""

# Subtract a reported count, never below zero: a stale or duplicate
# acknowledgement must not make later reports negative.
# KEYS: dropped counter. ARGV: count. Returns the amount subtracted.
_ACKNOWLEDGE_DROPPED = """
local current = tonumber(redis.call('get', KEYS[1])) or 0
local amount = math.min(tonumber(ARGV[1]), current)
if amount > 0 then
    redis.call('decrby', KEYS[1], amount)
    return amount
end
return 0
"""

# Release the lock if this flusher still holds it, first applying any cap that
# pushes deferred while it was held. KEYS: lock, buffer, dropped counter.
# ARGV: token, cap. Returns the number of records dropped.
_RELEASE_AND_CAP = """
if redis.call('get', KEYS[1]) ~= ARGV[1] then
    return 0
end
local excess = redis.call('llen', KEYS[2]) - tonumber(ARGV[2])
if excess > 0 then
    redis.call('ltrim', KEYS[2], excess, -1)
    redis.call('incrby', KEYS[3], excess)
else
    excess = 0
end
redis.call('del', KEYS[1])
return excess
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

    async def _release(self, integration_id, output_type, token: str, max_records: int):
        try:
            await self.db_client.eval(
                _RELEASE_AND_CAP, 3,
                _lock_key(integration_id, output_type), buffer_key(integration_id, output_type),
                _dropped_key(integration_id, output_type), token, max_records,
            )
        except Exception as e:
            # The lock expires on its own, and the next push applies the cap.
            logger.warning(f"Could not release the flush lock for '{buffer_key(integration_id, output_type)}': "
                           f"{type(e).__name__}: {e}")

    async def push(self, integration_id, output_type, record: Any, max_records: Optional[int] = None) -> PushResult:
        """Append a JSON-safe record. Past max_records the oldest records are
        dropped (and counted in `.dropped`); PushResult.dropped says how many.
        While a flush runs the cap is deferred to its release."""
        max_records = max_records or settings.OUTBOUND_BUFFER_MAX_RECORDS
        entry = json.dumps({"enqueued_at": time.time(), "record": record})
        length, dropped = await self.db_client.eval(
            _PUSH, 3,
            buffer_key(integration_id, output_type), _lock_key(integration_id, output_type),
            _dropped_key(integration_id, output_type), entry, max_records,
        )
        return PushResult(length=int(length), dropped=int(dropped))

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

    async def pending_dropped(self, integration_id, output_type) -> int:
        """Records dropped by the cap and not yet taken."""
        return max(0, int(await self.db_client.get(_dropped_key(integration_id, output_type)) or 0))

    async def acknowledge_dropped(self, integration_id, output_type, count: int) -> int:
        """Mark `count` dropped records as reported; returns the amount subtracted.
        Subtracts rather than deletes, so drops counted since they were read stay
        for the next report, and never goes below zero."""
        return int(await self.db_client.eval(
            _ACKNOWLEDGE_DROPPED, 1, _dropped_key(integration_id, output_type), count,
        ))

    async def length(self, integration_id, output_type) -> int:
        return await self.db_client.llen(buffer_key(integration_id, output_type))

    async def is_due(self, integration_id, output_type, max_batch_size: int, max_wait_seconds: float) -> bool:
        """Whether a flush would send now: a full batch, or a head older than max_wait_seconds."""
        return await self._is_due(buffer_key(integration_id, output_type), max_batch_size, max_wait_seconds)

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
            enqueued_at = float(json.loads(head)["enqueued_at"])
            if not math.isfinite(enqueued_at):
                raise ValueError("not a finite number")
        except (ValueError, KeyError, TypeError):
            # Due now: a head whose age cannot be read (or that would never age,
            # like NaN or inf) must not block the buffer forever.
            logger.warning(f"The oldest record in outbound buffer '{key}' has no usable enqueue time; flushing now.")
            return True
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
            max_records: Optional[int] = None,
    ) -> FlushResult:
        """Send batches while the buffer is due: holding a full batch, or a head
        older than max_wait_seconds. Returns without sending if another flush
        holds the lock. On release, success or not, applies the cap pushes
        deferred meanwhile. Exceptions from `send` propagate after the release."""
        max_records = max_records or settings.OUTBOUND_BUFFER_MAX_RECORDS
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
            await self._release(integration_id, output_type, token, max_records)
        return FlushResult(batches_sent=batches_sent, records_sent=records_sent)
