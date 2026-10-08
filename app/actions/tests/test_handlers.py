import json
import time

import pytest
from gundi_core.events import GundiDelivery, LogLevel

from app.actions import client, handlers
from app.actions.configurations import AuthenticateConfig, DeliverBatchConfig, DeliverConfig, FlushBuffersConfig
from app.actions.envelopes import GundiBatchDelivery
from app.actions.tests.conftest import (
    AUTH_CONFIG_DATA, PROVIDER, attachment, deliver_config_data, event, make_integration, observation, text_message,
)
from app import settings
from app.services.errors import IntegrationBadResponseError, IntegrationConfigurationError, IntegrationConnectionError
from app.services.outbound_buffer import buffer_key


def _delivery(payload):
    return GundiDelivery(payload=payload, provider=PROVIDER)


def _bundle(payloads, batch_id="batch-1"):
    return GundiBatchDelivery(batch_id=batch_id, provider=PROVIDER, payloads=payloads)


async def _deliver(payload, auth=AUTH_CONFIG_DATA, **config):
    data = deliver_config_data(**config)
    return await handlers.action_deliver(
        integration=make_integration(deliver=data, auth=auth),
        action_config=DeliverConfig.parse_obj(data),
        data=_delivery(payload),
        metadata={},
    )


async def _deliver_batch(bundle, auth=AUTH_CONFIG_DATA, **config):
    return await handlers.action_deliver_batch(
        integration=make_integration(deliver=deliver_config_data(**config), auth=auth),
        action_config=DeliverBatchConfig(),
        data=bundle,
        metadata={},
    )


def _bodies(send_json):
    return [call.kwargs["body"] for call in send_json.call_args_list]


def _titles(log_activity):
    return [call.kwargs["title"] for call in log_activity.call_args_list]


# Headers

def test_headers_without_auth_config():
    assert handlers.build_headers(None) == {"Content-Type": "application/json"}


def test_headers_with_prefixed_key_and_custom_headers():
    headers = handlers.build_headers(AuthenticateConfig.parse_obj(AUTH_CONFIG_DATA))
    assert headers == {"Content-Type": "application/json", "Authorization": "Bearer s3cret", "X-Tenant": "acme"}


def test_headers_with_raw_key_in_a_custom_header():
    config = AuthenticateConfig(api_key="k", api_key_header="X-Api-Key", api_key_prefix="")
    assert handlers.build_headers(config)["X-Api-Key"] == "k"


def test_configured_headers_replace_defaults_case_insensitively():
    config = AuthenticateConfig(custom_headers=[{"name": "content-type", "value": "application/vnd.x+json"}])
    assert handlers.build_headers(config) == {"content-type": "application/vnd.x+json"}


def test_no_api_key_means_no_auth_header():
    assert "Authorization" not in handlers.build_headers(AuthenticateConfig())


@pytest.mark.asyncio
async def test_auth_action_reports_valid_credentials():
    result = await handlers.action_auth(make_integration(), AuthenticateConfig.parse_obj(AUTH_CONFIG_DATA))
    assert result == {"valid_credentials": True}


# action_deliver, single mode

@pytest.mark.asyncio
async def test_deliver_single_sends_the_filtered_record(outbound_env):
    send_json, _ = outbound_env

    result = await _deliver(observation(1), observation_jq_filter="{id: .gundi_id, at: .recorded_at}")

    assert result == {"delivered": True, "output_type": "observation"}
    call = send_json.call_args.kwargs
    assert call["url"] == "https://hooks.example.com/observations"
    assert call["method"] == "POST"
    assert call["headers"]["Authorization"] == "Bearer s3cret"
    # JSON-safe values: the datetime was serialized before jq saw it.
    assert call["body"] == {"id": "00000000-0000-0000-0000-000000000001", "at": "2026-10-08T12:01:00+00:00"}


@pytest.mark.asyncio
async def test_deliver_skips_the_request_when_the_filter_outputs_nothing(outbound_env):
    send_json, _ = outbound_env

    result = await _deliver(observation(1), observation_jq_filter="select(.source_name == \"nobody\")")

    assert result == {"delivered": False, "output_type": "observation"}
    send_json.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("payload, payload_type", [(attachment(), "Attachment"), (text_message(), "TextMessage")])
async def test_deliver_drops_unsupported_or_unselected_types(outbound_env, payload, payload_type):
    send_json, log_activity = outbound_env

    result = await _deliver(payload)

    assert result == {"dropped": True, "payload_type": payload_type}
    send_json.assert_not_called()
    assert log_activity.call_args.kwargs["level"] == LogLevel.INFO
    assert payload_type in log_activity.call_args.kwargs["title"]


@pytest.mark.asyncio
async def test_deliver_raises_retryable_failures_for_redelivery(outbound_env):
    send_json, log_activity = outbound_env
    send_json.side_effect = client.EndpointServerError("POST to hooks.example.com answered HTTP 503", 503)

    with pytest.raises(Exception) as exc_info:
        await _deliver(event(1))

    assert exc_info.value.status_code == 503
    log_activity.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [
    client.EndpointRejectedError("POST to hooks.example.com answered HTTP 400", 400),
    client.EndpointAuthError("POST to hooks.example.com answered HTTP 401", 401),
])
async def test_deliver_logs_and_acks_permanent_failures(outbound_env, error):
    send_json, log_activity = outbound_env
    send_json.side_effect = error

    result = await _deliver(event(1))

    assert result["delivered"] is False
    assert log_activity.call_args.kwargs["level"] == LogLevel.ERROR
    assert log_activity.call_args.kwargs["data"]["gundi_ids"] == ["10000000-0000-0000-0000-000000000001"]


@pytest.mark.asyncio
async def test_deliver_refuses_private_addresses(outbound_env, mocker):
    send_json, log_activity = outbound_env
    mocker.patch("app.services.url_policy._resolve_addresses", mocker.AsyncMock(return_value=["10.0.0.5"]))

    result = await _deliver(event(1))

    assert result["delivered"] is False
    send_json.assert_not_called()
    assert "private or reserved address" in log_activity.call_args.kwargs["title"]


@pytest.mark.asyncio
async def test_deliver_reports_a_broken_filter_as_a_configuration_problem(outbound_env):
    send_json, log_activity = outbound_env

    result = await _deliver(event(1), event_jq_filter="{")

    assert result["delivered"] is False
    send_json.assert_not_called()
    assert log_activity.call_args.kwargs["data"]["error_type"] == "configuration"


@pytest.mark.asyncio
async def test_deliver_works_without_auth_config(outbound_env):
    send_json, _ = outbound_env

    await _deliver(event(1), auth=None)

    assert send_json.call_args.kwargs["headers"] == {"Content-Type": "application/json"}


# action_deliver, batch mode

@pytest.mark.asyncio
async def test_deliver_batch_mode_buffers_until_the_batch_is_full(outbound_env, fake_redis):
    send_json, _ = outbound_env
    config = dict(observation_batch_mode=True, observation_max_batch_size=3)

    first = await _deliver(observation(0), **config)
    await _deliver(observation(1), **config)

    assert first["buffered"] is True and first["buffer_length"] == 1
    send_json.assert_not_called()

    third = await _deliver(observation(2), **config)

    assert third["flush"]["batches_sent"] == 1
    body = send_json.call_args.kwargs["body"]
    assert [record["gundi_id"][-1] for record in body] == ["0", "1", "2"]
    assert fake_redis.lists[buffer_key(make_integration().id, "observation")] == []


@pytest.mark.asyncio
async def test_deliver_batch_mode_flush_failure_keeps_the_records_and_does_not_fail_the_run(outbound_env, fake_redis):
    send_json, log_activity = outbound_env
    send_json.side_effect = client.EndpointConnectionError("Could not reach hooks.example.com", None)

    result = await _deliver(observation(0), observation_batch_mode=True, observation_max_batch_size=1)

    assert result["buffered"] is True and "error" in result["flush"]
    assert len(fake_redis.lists[buffer_key(make_integration().id, "observation")]) == 1
    assert log_activity.call_args.kwargs["level"] == LogLevel.WARNING


# action_deliver_batch (bundles)

@pytest.mark.asyncio
@pytest.mark.parametrize("items, max_batch_size, expected_sizes", [
    (30, 10, [10, 10, 10]),
    (10, 20, [10]),
    (25, 10, [10, 10, 5]),
])
async def test_bundle_is_chunked_by_max_batch_size(outbound_env, items, max_batch_size, expected_sizes):
    send_json, _ = outbound_env

    result = await _deliver_batch(
        _bundle([observation(n) for n in range(items)]),
        observation_batch_mode=True, observation_max_batch_size=max_batch_size,
    )

    assert [len(body) for body in _bodies(send_json)] == expected_sizes
    assert result["delivered"]["observation"]["sent"] == len(expected_sizes)


@pytest.mark.asyncio
async def test_bundle_in_single_mode_sends_one_request_per_record(outbound_env):
    send_json, _ = outbound_env

    await _deliver_batch(_bundle([event(n) for n in range(3)]), event_jq_filter="{t: .title}")

    assert _bodies(send_json) == [{"t": "Sighting 0"}, {"t": "Sighting 1"}, {"t": "Sighting 2"}]


@pytest.mark.asyncio
async def test_bundle_groups_by_type_and_drops_the_rest(outbound_env):
    send_json, log_activity = outbound_env

    result = await _deliver_batch(_bundle([observation(0), event(0), attachment(0), text_message(0), observation(1)]))

    assert sorted(call.kwargs["url"] for call in send_json.call_args_list) == [
        "https://hooks.example.com/events",
        "https://hooks.example.com/observations",
        "https://hooks.example.com/observations",
    ]
    assert result["dropped"] == 2
    assert _titles(log_activity) == ["Dropping 1 Attachment, 1 TextMessage: not a data type selected for delivery."]


@pytest.mark.asyncio
async def test_bundle_batch_filter_receives_the_chunk_array(outbound_env):
    send_json, _ = outbound_env

    await _deliver_batch(
        _bundle([observation(n) for n in range(4)]),
        observation_batch_mode=True, observation_max_batch_size=2,
        observation_jq_filter="{count: length, ids: map(.external_source_id)}",
    )

    assert _bodies(send_json) == [
        {"count": 2, "ids": ["collar-0", "collar-1"]},
        {"count": 2, "ids": ["collar-2", "collar-3"]},
    ]


@pytest.mark.asyncio
async def test_retryable_failure_raises_and_redelivery_skips_delivered_chunks(outbound_env):
    send_json, _ = outbound_env
    bundle = _bundle([observation(n) for n in range(30)])
    config = dict(observation_batch_mode=True, observation_max_batch_size=10)
    send_json.side_effect = [None, client.EndpointServerError("HTTP 502", 502)]

    with pytest.raises(Exception) as exc_info:
        await _deliver_batch(bundle, **config)
    assert exc_info.value.status_code == 502

    # Pub/Sub redelivers the same bundle: chunk 0 is not sent again.
    send_json.reset_mock(side_effect=True)
    result = await _deliver_batch(bundle, **config)

    assert [body[0]["external_source_id"] for body in _bodies(send_json)] == ["collar-10", "collar-20"]
    assert result["delivered"]["observation"]["already_settled"] == 1


@pytest.mark.asyncio
async def test_a_fully_delivered_bundle_is_not_resent(outbound_env):
    send_json, _ = outbound_env
    bundle = _bundle([event(n) for n in range(3)])

    await _deliver_batch(bundle)
    send_json.reset_mock()
    result = await _deliver_batch(bundle)

    send_json.assert_not_called()
    assert result["delivered"]["event"]["already_settled"] == 3


@pytest.mark.asyncio
async def test_a_config_change_between_deliveries_invalidates_progress(outbound_env):
    send_json, _ = outbound_env
    bundle = _bundle([observation(n) for n in range(4)])

    await _deliver_batch(bundle, observation_batch_mode=True, observation_max_batch_size=2)
    send_json.reset_mock()
    await _deliver_batch(bundle, observation_batch_mode=True, observation_max_batch_size=4)

    # Different chunking: old bits name other requests, so everything is sent (duplicates, not loss).
    assert [len(body) for body in _bodies(send_json)] == [4]


@pytest.mark.asyncio
async def test_permanent_failure_is_logged_and_the_rest_continues(outbound_env):
    send_json, log_activity = outbound_env
    send_json.side_effect = [None, client.EndpointRejectedError("HTTP 422", 422), None]

    result = await _deliver_batch(
        _bundle([observation(n) for n in range(3)]),
        observation_batch_mode=True, observation_max_batch_size=1,
    )

    assert send_json.call_count == 3
    assert result["delivered"]["observation"] == {
        "requests": 3, "already_settled": 0, "sent": 2, "skipped_by_filter": 0, "failed": 1,
    }
    assert log_activity.call_args.kwargs["level"] == LogLevel.ERROR
    assert log_activity.call_args.kwargs["data"]["gundi_ids"] == ["00000000-0000-0000-0000-000000000001"]


@pytest.mark.asyncio
@pytest.mark.parametrize("deliver", [None, {"output_types": []}])
async def test_bundle_without_a_usable_deliver_config_is_logged_and_acked(outbound_env, deliver):
    send_json, log_activity = outbound_env

    result = await handlers.action_deliver_batch(
        integration=make_integration(deliver=deliver), action_config=DeliverBatchConfig(),
        data=_bundle([event(0), event(1)]), metadata={},
    )

    assert result == {"batch_id": "batch-1", "dropped": 2, "reason": "deliver_not_configured"}
    send_json.assert_not_called()
    assert log_activity.call_args.kwargs["level"] == LogLevel.ERROR
    assert log_activity.call_args.kwargs["data"]["batch_id"] == "batch-1"


# action_flush_buffers

async def _push_aged(fake_redis, records, age_seconds):
    key = buffer_key(make_integration().id, "observation")
    entries = [json.dumps({"enqueued_at": time.time() - age_seconds, "record": r}).encode() for r in records]
    fake_redis.lists.setdefault(key, []).extend(entries)
    return key


@pytest.mark.asyncio
async def test_flush_sends_aged_partial_buffers(outbound_env, fake_redis):
    send_json, _ = outbound_env
    key = await _push_aged(fake_redis, [observation(0), observation(1)], age_seconds=120)
    integration = make_integration(deliver=deliver_config_data(
        observation_batch_mode=True, observation_max_batch_size=10, observation_max_wait_seconds=60,
    ))

    result = await handlers.action_flush_buffers(integration=integration, action_config=FlushBuffersConfig())

    assert result["flushed"]["observation"]["records_sent"] == 2
    assert len(send_json.call_args.kwargs["body"]) == 2
    assert fake_redis.lists[key] == []


@pytest.mark.asyncio
async def test_flush_leaves_young_buffers(outbound_env, fake_redis):
    send_json, _ = outbound_env
    key = await _push_aged(fake_redis, [observation(0)], age_seconds=10)
    integration = make_integration(deliver=deliver_config_data(observation_batch_mode=True))

    await handlers.action_flush_buffers(integration=integration, action_config=FlushBuffersConfig())

    send_json.assert_not_called()
    assert len(fake_redis.lists[key]) == 1


@pytest.mark.asyncio
async def test_flush_skips_a_buffer_another_flusher_holds(outbound_env, fake_redis):
    send_json, _ = outbound_env
    key = await _push_aged(fake_redis, [observation(0)], age_seconds=120)
    await fake_redis.set(f"{key}.lock", "other", nx=True, ex=300)
    integration = make_integration(deliver=deliver_config_data(observation_batch_mode=True))

    result = await handlers.action_flush_buffers(integration=integration, action_config=FlushBuffersConfig())

    assert result["flushed"]["observation"]["locked_out"] is True
    send_json.assert_not_called()


@pytest.mark.asyncio
async def test_flush_drops_a_permanently_rejected_batch(outbound_env, fake_redis):
    send_json, log_activity = outbound_env
    send_json.side_effect = client.EndpointRejectedError("HTTP 400", 400)
    key = await _push_aged(fake_redis, [observation(0)], age_seconds=120)
    integration = make_integration(deliver=deliver_config_data(observation_batch_mode=True))

    await handlers.action_flush_buffers(integration=integration, action_config=FlushBuffersConfig())

    assert fake_redis.lists[key] == []
    assert log_activity.call_args.kwargs["level"] == LogLevel.ERROR


@pytest.mark.asyncio
async def test_flush_is_a_quiet_no_op_without_a_deliver_config(outbound_env):
    send_json, log_activity = outbound_env

    result = await handlers.action_flush_buffers(integration=make_integration(), action_config=FlushBuffersConfig())

    assert result == {"skipped": True, "reason": "deliver_not_configured"}
    send_json.assert_not_called()
    log_activity.assert_not_called()


@pytest.mark.asyncio
async def test_flush_warns_about_an_invalid_deliver_config_at_most_once_an_hour(outbound_env):
    send_json, log_activity = outbound_env
    integration = make_integration(deliver={"output_types": []})

    for _ in range(3):
        result = await handlers.action_flush_buffers(integration=integration, action_config=FlushBuffersConfig())

    assert result == {"skipped": True, "reason": "invalid_deliver_configuration"}
    send_json.assert_not_called()
    assert log_activity.call_count == 1
    assert log_activity.call_args.kwargs["level"] == LogLevel.WARNING


def test_flush_runs_every_minute():
    schedule = handlers.action_flush_buffers.crontab_schedule
    assert (schedule.minute, schedule.hour) == ("*", "*")


def test_retryability_is_set_where_the_failure_is_raised():
    assert handlers._is_retryable(handlers._failure(IntegrationConnectionError, "x", retryable=True))
    assert not handlers._is_retryable(handlers._failure(IntegrationConnectionError, "x", retryable=False))
    assert not handlers._is_retryable(IntegrationConfigurationError("x"))


@pytest.mark.asyncio
@pytest.mark.parametrize("payload, output_type", [
    ({"gundi_id": "e1", "changes": {"state": "resolved"}, "observation_type": "evu"}, "event_update"),
    (text_message(1), "message"),
])
async def test_event_updates_and_messages_go_to_their_own_endpoints(outbound_env, payload, output_type):
    send_json, _ = outbound_env
    url = f"https://hooks.example.com/{output_type}"

    result = await _deliver(payload, output_types=[output_type], **{f"{output_type}_url": url})

    assert result == {"delivered": True, "output_type": output_type}
    assert send_json.call_args.kwargs["url"] == url


# Missing URLs, request errors, DNS failures

@pytest.mark.asyncio
async def test_deliver_drops_a_selected_type_without_a_url(outbound_env):
    send_json, log_activity = outbound_env

    result = await _deliver(text_message(1), output_types=["message"])

    assert result == {"dropped": True, "output_type": "message", "reason": "missing_url"}
    send_json.assert_not_called()
    assert log_activity.call_args.kwargs["level"] == LogLevel.ERROR
    assert "Messages: URL" in log_activity.call_args.kwargs["title"]


@pytest.mark.asyncio
async def test_bundle_drops_only_the_types_without_a_url(outbound_env):
    send_json, log_activity = outbound_env

    result = await _deliver_batch(
        _bundle([event(0), text_message(0), text_message(1)]), output_types=["event", "message"],
    )

    assert result["delivered"]["message"] == {"dropped": 2, "reason": "missing_url"}
    assert result["delivered"]["event"]["sent"] == 1
    assert send_json.call_count == 1
    assert log_activity.call_args.kwargs["data"]["gundi_ids"] == [
        "20000000-0000-0000-0000-000000000000", "20000000-0000-0000-0000-000000000001",
    ]


@pytest.mark.asyncio
async def test_a_dns_failure_is_retried_not_dropped(outbound_env, mocker):
    send_json, log_activity = outbound_env
    mocker.patch("app.services.url_policy._resolve_addresses", mocker.AsyncMock(side_effect=OSError("EAI_AGAIN")))

    with pytest.raises(IntegrationConnectionError):
        await _deliver(event(1))

    send_json.assert_not_called()
    log_activity.assert_not_called()


@pytest.mark.asyncio
async def test_a_request_the_client_cannot_build_is_logged_and_acked(outbound_env):
    send_json, log_activity = outbound_env
    send_json.side_effect = client.EndpointRequestError("Could not build the request to hooks.example.com", None)

    result = await _deliver(event(1))

    assert result["delivered"] is False
    assert log_activity.call_args.kwargs["data"]["error_type"] == "configuration"


# Activity events: errors only, and never the endpoint URL

SECRET_URL = "https://hooks.slack.com/services/T0/B0/XXXXSECRET?token=abc"


@pytest.mark.asyncio
async def test_a_successful_delivery_publishes_no_activity_events(outbound_env, published_events):
    await _deliver(event(1))
    await _deliver_batch(_bundle([event(2)]))

    published_events.assert_not_called()


@pytest.mark.asyncio
async def test_no_url_path_or_query_reaches_activity_events(outbound_env, published_events):
    send_json, log_activity = outbound_env
    send_json.side_effect = [
        client.EndpointServerError("POST to hooks.slack.com answered HTTP 503", 503),
        client.EndpointRejectedError("POST to hooks.slack.com answered HTTP 400", 400),
    ]

    with pytest.raises(IntegrationBadResponseError):
        await _deliver(event(1), event_url=SECRET_URL)
    await _deliver(event(2), event_url=SECRET_URL)

    published_events.assert_called()  # the decorator's IntegrationActionFailed
    published = json.dumps(
        [c.kwargs["event"].dict() for c in published_events.call_args_list]
        + [c.kwargs for c in log_activity.call_args_list],
        default=str,
    )
    assert "XXXXSECRET" not in published and "token=abc" not in published
    assert "/services/" not in published


# Buffer backoff and cap

def _buffer_records(fake_redis, output_type="observation"):
    return [json.loads(e)["record"] for e in fake_redis.lists.get(buffer_key(make_integration().id, output_type), [])]


@pytest.mark.asyncio
async def test_a_failed_flush_backs_off_instead_of_retrying_per_record(outbound_env, fake_redis):
    send_json, log_activity = outbound_env
    send_json.side_effect = client.EndpointServerError("HTTP 503", 503)
    config = dict(observation_batch_mode=True, observation_max_batch_size=1)

    first = await _deliver(observation(0), **config)
    second = await _deliver(observation(1), **config)

    assert send_json.call_count == 1
    assert first["flush"]["retry_in_seconds"] == settings.OUTBOUND_BACKOFF_INITIAL_SECONDS
    assert "backing_off_seconds" in second["flush"]
    assert [c.kwargs["level"] for c in log_activity.call_args_list] == [LogLevel.WARNING]
    assert len(_buffer_records(fake_redis)) == 2


@pytest.mark.asyncio
async def test_a_429_backs_off_for_its_retry_after(outbound_env):
    send_json, log_activity = outbound_env
    send_json.side_effect = client.EndpointRateLimitError("HTTP 429", 429, retry_after=300)

    result = await _deliver(observation(0), observation_batch_mode=True, observation_max_batch_size=1)

    assert result["flush"]["retry_in_seconds"] == 300
    assert "in 300 s" in log_activity.call_args.kwargs["title"]


@pytest.mark.asyncio
async def test_a_successful_flush_ends_the_backoff_streak(outbound_env, fake_redis):
    send_json, _ = outbound_env
    integration_id = str(make_integration().id)
    await handlers.outbound_buffer.start_backoff(integration_id, "observation")
    await handlers.outbound_buffer.start_backoff(integration_id, "observation")
    await fake_redis.delete(f"{buffer_key(integration_id, 'observation')}.backoff")  # the window has passed

    await _deliver(observation(0), observation_batch_mode=True, observation_max_batch_size=1)

    assert send_json.call_count == 1
    assert f"{buffer_key(integration_id, 'observation')}.failures" not in fake_redis.values


@pytest.mark.asyncio
async def test_a_full_buffer_drops_its_oldest_records_with_an_error(outbound_env, fake_redis, mocker):
    send_json, log_activity = outbound_env
    mocker.patch.object(settings, "OUTBOUND_BUFFER_MAX_RECORDS", 2)
    config = dict(observation_batch_mode=True, observation_max_batch_size=10)

    for n in range(3):
        result = await _deliver(observation(n), **config)

    send_json.assert_not_called()
    assert result["buffer_length"] == 2
    assert [r["external_source_id"] for r in _buffer_records(fake_redis)] == ["collar-1", "collar-2"]
    assert log_activity.call_args.kwargs["level"] == LogLevel.ERROR
    assert log_activity.call_args.kwargs["data"]["dropped"] == 1


# Buffers left behind by a config change

@pytest.mark.asyncio
async def test_flush_drains_a_buffer_whose_type_left_batch_mode_one_record_per_request(outbound_env, fake_redis):
    send_json, _ = outbound_env
    key = await _push_aged(fake_redis, [observation(0), observation(1)], age_seconds=1)
    integration = make_integration(deliver=deliver_config_data(observation_jq_filter="{id: .external_source_id}"))

    await handlers.action_flush_buffers(integration=integration, action_config=FlushBuffersConfig())

    assert _bodies(send_json) == [{"id": "collar-0"}, {"id": "collar-1"}]
    assert fake_redis.lists[key] == []


@pytest.mark.asyncio
async def test_flush_drains_a_deselected_type_that_still_has_a_url(outbound_env, fake_redis):
    send_json, _ = outbound_env
    key = await _push_aged(fake_redis, [observation(0)], age_seconds=1)
    integration = make_integration(deliver=deliver_config_data(
        output_types=["event"], observation_batch_mode=True,
    ))

    await handlers.action_flush_buffers(integration=integration, action_config=FlushBuffersConfig())

    assert len(send_json.call_args.kwargs["body"]) == 1
    assert fake_redis.lists[key] == []


@pytest.mark.asyncio
async def test_flush_drops_a_left_behind_buffer_with_no_url_and_says_so(outbound_env, fake_redis):
    send_json, log_activity = outbound_env
    key = await _push_aged(fake_redis, [observation(0), observation(1)], age_seconds=1)
    integration = make_integration(deliver={"output_types": ["event"], "event_url": "https://hooks.example.com/e"})

    result = await handlers.action_flush_buffers(integration=integration, action_config=FlushBuffersConfig())

    assert result["flushed"]["observation"]["dropped"] is True
    send_json.assert_not_called()
    assert fake_redis.lists[key] == []
    assert log_activity.call_args.kwargs["level"] == LogLevel.ERROR
    assert len(log_activity.call_args.kwargs["data"]["gundi_ids"]) == 2


@pytest.mark.asyncio
async def test_a_failure_mid_drain_does_not_resend_delivered_records(outbound_env, fake_redis):
    send_json, _ = outbound_env
    key = await _push_aged(fake_redis, [observation(n) for n in range(5)], age_seconds=1)
    integration = make_integration(deliver=deliver_config_data(observation_jq_filter=".external_source_id"))
    send_json.side_effect = [None, None, None, client.EndpointServerError("HTTP 503", 503), None, None]

    await handlers.action_flush_buffers(integration=integration, action_config=FlushBuffersConfig())
    await handlers.outbound_buffer.clear_backoff(str(integration.id), "observation")  # the window has passed
    await handlers.action_flush_buffers(integration=integration, action_config=FlushBuffersConfig())

    assert _bodies(send_json) == ["collar-0", "collar-1", "collar-2", "collar-3", "collar-3", "collar-4"]
    assert fake_redis.lists[key] == []


@pytest.mark.asyncio
async def test_buffer_overflow_errors_are_throttled_and_carry_the_accumulated_count(outbound_env, mocker):
    send_json, log_activity = outbound_env
    mocker.patch.object(settings, "OUTBOUND_BUFFER_MAX_RECORDS", 2)
    # Report window open on the first drop, closed for two, open again on the fourth.
    handlers.state_manager.set_if_absent = mocker.AsyncMock(side_effect=[True, False, False, True])
    config = dict(observation_batch_mode=True, observation_max_batch_size=10)

    for n in range(6):
        await _deliver(observation(n), **config)

    reports = [c.kwargs for c in log_activity.call_args_list if c.kwargs["level"] == LogLevel.ERROR]
    assert [r["data"]["dropped"] for r in reports] == [1, 3]
    assert "dropped 3 of its oldest record(s) since the last report" in reports[1]["title"]
