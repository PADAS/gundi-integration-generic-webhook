import json
from datetime import datetime

import pytest

from app import settings
from app.services.outbound_samples import OutboundSamples, samples_key
from app.services.tests.fake_redis import FakeRedis

INTEGRATION_ID = "0f2b3c1e-6a6e-4d36-9d59-6f3c1e2b9a10"
KEY = samples_key(INTEGRATION_ID, "event")


@pytest.fixture
def redis():
    return FakeRedis()


@pytest.fixture
def samples(redis):
    return OutboundSamples(db_client=redis)


@pytest.mark.asyncio
async def test_keeps_the_newest_records_newest_first_with_a_ttl(samples, redis):
    for n in range(5):
        await samples.capture(INTEGRATION_ID, "event", [{"n": n}])

    stored = await samples.read(INTEGRATION_ID, "event")

    assert [s["record"] for s in stored] == [{"n": 4}, {"n": 3}, {"n": 2}]
    assert redis.ttls[KEY] == settings.OUTBOUND_SAMPLES_TTL_SECONDS == 172800
    assert datetime.fromisoformat(stored[0]["captured_at"]).utcoffset().total_seconds() == 0


@pytest.mark.asyncio
async def test_a_bundle_writes_only_its_newest_records(samples, redis):
    assert await samples.capture(INTEGRATION_ID, "event", [{"n": n} for n in range(100)]) == 3

    assert [s["record"]["n"] for s in await samples.read(INTEGRATION_ID, "event")] == [99, 98, 97]
    assert len(redis.lists[KEY]) == 3


@pytest.mark.asyncio
async def test_oversized_records_are_skipped(samples, mocker):
    mocker.patch.object(settings, "OUTBOUND_SAMPLE_MAX_BYTES", 100)
    big, small = {"text": "x" * 200}, {"text": "ok"}

    assert await samples.capture(INTEGRATION_ID, "event", [small, big]) == 1
    assert await samples.capture(INTEGRATION_ID, "event", [big]) == 0

    assert [s["record"] for s in await samples.read(INTEGRATION_ID, "event")] == [small]


@pytest.mark.asyncio
async def test_clear_deletes_the_samples(samples):
    await samples.capture(INTEGRATION_ID, "event", [{"n": 1}])
    await samples.clear(INTEGRATION_ID, "event")
    assert await samples.read(INTEGRATION_ID, "event") == []


@pytest.mark.asyncio
async def test_unreadable_entries_are_skipped(samples, redis):
    await samples.capture(INTEGRATION_ID, "event", [{"n": 1}])
    redis.lists[KEY].insert(0, b"not json")
    redis.lists[KEY].insert(0, json.dumps({"record": {}}).encode())

    assert [s["record"] for s in await samples.read(INTEGRATION_ID, "event")] == [{"n": 1}]
