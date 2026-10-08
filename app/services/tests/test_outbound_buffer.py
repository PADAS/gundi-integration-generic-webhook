import json
import time

import pytest

from app.services import outbound_buffer
from app.services.outbound_buffer import OutboundBuffer, buffer_key
from app.services.tests.fake_redis import FakeRedis

KEY = buffer_key("integration-1", "observation")
LOCK_KEY = f"{KEY}.lock"


@pytest.fixture
def redis():
    return FakeRedis()


@pytest.fixture
def buffer(redis):
    return OutboundBuffer(db_client=redis)


class Recorder:
    def __init__(self, fail_with=None):
        self.batches = []
        self.fail_with = fail_with

    async def __call__(self, records):
        if self.fail_with:
            raise self.fail_with
        self.batches.append(records)


async def _flush(buffer, send, max_batch_size=3, max_wait_seconds=60):
    return await buffer.flush(
        "integration-1", "observation",
        max_batch_size=max_batch_size, max_wait_seconds=max_wait_seconds, send=send,
        lock_seconds=300, request_timeout=30,
    )


def _age_head(redis, seconds):
    entry = json.loads(redis.lists[KEY][0])
    entry["enqueued_at"] = time.time() - seconds
    redis.lists[KEY][0] = json.dumps(entry).encode()


@pytest.mark.asyncio
async def test_push_returns_the_buffer_length(buffer):
    assert (await buffer.push("integration-1", "observation", {"n": 1})).length == 1
    assert await buffer.push("integration-1", "observation", {"n": 2}) == (2, 0)


@pytest.mark.asyncio
async def test_push_past_the_cap_drops_the_oldest(buffer, redis):
    for n in range(3):
        await buffer.push("integration-1", "observation", {"n": n}, max_records=3)

    result = await buffer.push("integration-1", "observation", {"n": 3}, max_records=3)

    assert result == (3, 1)
    assert [json.loads(e)["record"]["n"] for e in redis.lists[KEY]] == [1, 2, 3]
    assert LOCK_KEY not in redis.values
    # Short-lived, so a crash mid-trim does not block flushes for the full flush lock.
    assert redis.ttls[LOCK_KEY] == outbound_buffer.CAP_TRIM_LOCK_SECONDS


@pytest.mark.asyncio
async def test_dropped_counts_accumulate_until_taken(buffer):
    await buffer.add_dropped("integration-1", "observation", 2)
    await buffer.add_dropped("integration-1", "observation", 3)

    assert await buffer.take_dropped("integration-1", "observation") == 5
    assert await buffer.take_dropped("integration-1", "observation") == 0


@pytest.mark.asyncio
async def test_push_past_the_cap_leaves_trimming_to_a_running_flush(buffer, redis):
    # Trimming the head under a flush would drop records the flush read but has not sent.
    for n in range(3):
        await buffer.push("integration-1", "observation", {"n": n}, max_records=3)
    await redis.set(LOCK_KEY, "flusher", nx=True, ex=300)

    result = await buffer.push("integration-1", "observation", {"n": 3}, max_records=3)

    assert result == (4, 0)
    assert len(redis.lists[KEY]) == 4


@pytest.mark.asyncio
async def test_backoff_doubles_up_to_the_cap_and_honours_retry_after(buffer, mocker):
    mocker.patch.object(outbound_buffer.settings, "OUTBOUND_BACKOFF_INITIAL_SECONDS", 30)
    mocker.patch.object(outbound_buffer.settings, "OUTBOUND_BACKOFF_MAX_SECONDS", 100)

    delays = [await buffer.start_backoff("integration-1", "observation") for _ in range(4)]

    assert delays == [30, 60, 100, 100]
    assert 99 <= await buffer.backoff_remaining("integration-1", "observation") <= 100
    assert await buffer.start_backoff("integration-1", "observation", retry_after=7) == 7
    await buffer.clear_backoff("integration-1", "observation")
    assert await buffer.backoff_remaining("integration-1", "observation") == 0
    assert await buffer.start_backoff("integration-1", "observation") == 30  # streak reset


@pytest.mark.asyncio
async def test_a_young_partial_buffer_is_not_flushed(buffer, redis):
    await buffer.push("integration-1", "observation", {"n": 1})
    send = Recorder()

    result = await _flush(buffer, send)

    assert send.batches == []
    assert result.batches_sent == 0
    assert len(redis.lists[KEY]) == 1


@pytest.mark.asyncio
async def test_full_batches_are_flushed_and_the_young_remainder_kept(buffer, redis):
    for n in range(7):
        await buffer.push("integration-1", "observation", {"n": n})
    send = Recorder()

    result = await _flush(buffer, send, max_batch_size=3)

    assert send.batches == [[{"n": 0}, {"n": 1}, {"n": 2}], [{"n": 3}, {"n": 4}, {"n": 5}]]
    assert (result.batches_sent, result.records_sent) == (2, 6)
    assert [json.loads(e)["record"] for e in redis.lists[KEY]] == [{"n": 6}]
    assert LOCK_KEY not in redis.values  # released


@pytest.mark.asyncio
async def test_an_aged_buffer_is_drained(buffer, redis):
    for n in range(4):
        await buffer.push("integration-1", "observation", {"n": n})
    _age_head(redis, 120)
    send = Recorder()

    await _flush(buffer, send, max_batch_size=3, max_wait_seconds=60)

    # The aged head goes out with the full batch; the record left behind is young.
    assert send.batches == [[{"n": 0}, {"n": 1}, {"n": 2}]]
    _age_head(redis, 120)
    await _flush(buffer, send, max_batch_size=3, max_wait_seconds=60)
    assert send.batches[-1] == [{"n": 3}]
    assert redis.lists[KEY] == []


@pytest.mark.asyncio
async def test_lock_contention_skips_the_flush(buffer, redis):
    for n in range(3):
        await buffer.push("integration-1", "observation", {"n": n})
    await redis.set(LOCK_KEY, "someone-else", nx=True, ex=300)
    send = Recorder()

    result = await _flush(buffer, send)

    assert result.locked_out
    assert send.batches == []
    assert len(redis.lists[KEY]) == 3
    assert redis.values[LOCK_KEY] == b"someone-else"  # not released by us


@pytest.mark.asyncio
async def test_a_failed_send_keeps_the_records_and_releases_the_lock(buffer, redis):
    for n in range(3):
        await buffer.push("integration-1", "observation", {"n": n})

    with pytest.raises(RuntimeError):
        await _flush(buffer, Recorder(fail_with=RuntimeError("endpoint down")))

    assert len(redis.lists[KEY]) == 3
    assert LOCK_KEY not in redis.values


@pytest.mark.asyncio
async def test_a_flusher_that_lost_its_lock_does_not_trim(buffer, redis):
    for n in range(6):
        await buffer.push("integration-1", "observation", {"n": n})

    async def send_while_lock_expires(records):
        # Our lock expired mid-send and another flusher took it.
        redis.values[LOCK_KEY] = b"other-flusher"

    result = await _flush(buffer, send_while_lock_expires)

    assert result.batches_sent == 0
    assert len(redis.lists[KEY]) == 6
    assert redis.values[LOCK_KEY] == b"other-flusher"


@pytest.mark.asyncio
async def test_unreadable_entries_do_not_block_the_buffer(buffer, redis):
    redis.lists[KEY] = [b"not json", json.dumps({"enqueued_at": time.time(), "record": {"n": 1}}).encode()]
    send = Recorder()

    await _flush(buffer, send, max_batch_size=3)

    assert send.batches == [[{"n": 1}]]
    assert redis.lists[KEY] == []


@pytest.mark.asyncio
async def test_records_appended_during_a_send_are_neither_lost_nor_resent(buffer, redis):
    # Exercises the fake's emulation of the trim script, not the Lua on a real Redis.
    for n in range(3):
        await buffer.push("integration-1", "observation", {"n": n})
    sent = []

    async def send_while_others_append(records):
        sent.append([r["n"] for r in records])
        if len(sent) == 1:
            for n in range(3, 5):
                await buffer.push("integration-1", "observation", {"n": n})

    await _flush(buffer, send_while_others_append, max_batch_size=3)
    _age_head(redis, 120)
    await _flush(buffer, send_while_others_append, max_batch_size=3)

    assert sent == [[0, 1, 2], [3, 4]]
    assert redis.lists[KEY] == []
