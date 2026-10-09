import pytest

from app.services import batch_progress
from app.services.batch_progress import BatchProgressStore, decode, encode, fingerprint, headers_digest
from app.services.tests.fake_redis import FakeRedis


def test_fingerprint_is_injective_where_a_delimiter_join_is_not():
    assert fingerprint(["a|b", "c"], 10) != fingerprint(["a", "b|c"], 10)


def test_fingerprint_binds_order_and_chunk_size():
    assert fingerprint(["a", "b"], 10) == fingerprint(["a", "b"], 10)
    assert fingerprint(["a", "b"], 10) != fingerprint(["b", "a"], 10)
    # Same items chunked differently: bit i no longer names the same request.
    assert fingerprint(["a", "b"], 10) != fingerprint(["a", "b"], 1)


def test_fingerprint_binds_the_request_plan():
    plan = ["https://h/x", "POST", ".", "single", headers_digest({"Authorization": "Bearer k"})]
    assert fingerprint(["a"], 1, plan) == fingerprint(["a"], 1, list(plan))
    for i, changed in enumerate(["https://h/y", "PUT", "{a}", "batch", headers_digest({"Authorization": "Bearer j"})]):
        assert fingerprint(["a"], 1, plan[:i] + [changed] + plan[i + 1:]) != fingerprint(["a"], 1, plan)
    assert fingerprint(["a"], 1, plan) != fingerprint(["a"], 1)


def test_plan_fields_are_length_prefixed_too():
    assert fingerprint(["a"], 1, ["ab", "c"]) != fingerprint(["a"], 1, ["a", "bc"])
    # Ids and plan are separate sequences: moving a field between them changes the digest.
    assert fingerprint(["a", "b"], 1, []) != fingerprint(["a"], 1, ["b"])


def test_headers_digest_ignores_name_case_and_order_but_not_values():
    assert headers_digest({"A": "1", "b": "2"}) == headers_digest({"B": "2", "a": "1"})
    assert headers_digest({"A": "1"}) != headers_digest({"A": "2"})
    assert headers_digest({"A": "1", "B": ""}) != headers_digest({"A": "1"})
    assert b"secret" not in headers_digest({"Authorization": "secret"})


@pytest.mark.parametrize("chunk_size", [2 ** 32 - 1, 2 ** 32, 2 ** 64, 10 ** 30])
def test_fingerprint_takes_any_chunk_size(chunk_size):
    # A fixed-width field would overflow here and fail every delivery of the bundle.
    assert len(fingerprint(["a"], chunk_size)) == batch_progress.FINGERPRINT_BYTES
    assert fingerprint(["a"], chunk_size) != fingerprint(["a"], chunk_size + 1)


def test_encode_decode_round_trip():
    fp = fingerprint(["a"] * 12, 1)
    raw = encode(fp, {0, 3, 11}, 12)
    assert decode(raw, fp, 12) == {0, 3, 11}


def test_out_of_range_indices_are_not_encoded():
    fp = fingerprint(["a", "b"], 1)
    assert decode(encode(fp, {1, 5, -1}, 2), fp, 2) == {1}


@pytest.mark.parametrize("raw", [None, b"", b"short"])
def test_missing_or_truncated_records_decode_empty(raw):
    assert decode(raw, fingerprint(["a"], 1), 1) == set()


def test_record_for_a_different_plan_decodes_empty():
    raw = encode(fingerprint(["a", "b"], 10), {0}, 1)
    assert decode(raw, fingerprint(["a", "b"], 1), 2) == set()


def test_decode_fails_open_on_an_unexpected_type():
    assert decode(12345, fingerprint(["a"], 1), 1) == set()
    assert decode(b"x" * 9, b"short", 1) == set()


@pytest.mark.asyncio
async def test_store_round_trip_with_ttl(mocker):
    mocker.patch.object(batch_progress.settings, "OUTBOUND_BATCH_PROGRESS_TTL_SECONDS", 90000)
    redis = FakeRedis()
    store = BatchProgressStore(db_client=redis)
    fp = fingerprint(["a", "b", "c"], 1)

    await store.write("batch-1", "integration-1", "observation", fp, {0, 2}, 3)

    raw = await store.read("batch-1", "integration-1", "observation")
    assert decode(raw, fp, 3) == {0, 2}
    assert redis.ttls["outbound_batch_progress.batch-1.integration-1.observation"] == 90000
    assert await store.read("batch-1", "integration-1", "event") is None


@pytest.mark.asyncio
async def test_store_skips_writing_nothing():
    redis = FakeRedis()
    await BatchProgressStore(db_client=redis).write("b", "i", "event", fingerprint([], 1), set(), 0)
    assert redis.values == {}


@pytest.mark.asyncio
async def test_store_never_raises(mocker):
    redis = mocker.MagicMock()
    redis.get = mocker.AsyncMock(side_effect=ConnectionError("down"))
    redis.setex = mocker.AsyncMock(side_effect=ConnectionError("down"))
    store = BatchProgressStore(db_client=redis)

    assert await store.read("b", "i", "event") is None
    await store.write("b", "i", "event", fingerprint(["a"], 1), {0}, 1)
