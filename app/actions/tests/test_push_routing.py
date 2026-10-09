"""Both envelopes reach their handler through the runner, routed by event_type."""
import base64
import json

import pytest
from fastapi.testclient import TestClient
from gundi_core.events import GundiDelivery

from app.actions import client
from app.actions.core import get_actions
from app.actions.envelopes import GundiBatchDelivery
from app.actions.tests.conftest import (
    AUTH_CONFIG_DATA, INTEGRATION_ID, PROVIDER, deliver_config_data, event, make_integration, observation,
)
from app.conftest import async_return
from app.main import app
from app.services.self_registration import register_integration_in_gundi

api_client = TestClient(app)


SECRET_URL = "https://hooks.slack.com/services/T0/B0/XXXXSECRET?token=abc"


@pytest.fixture
def runner(mocker, outbound_env):
    data = deliver_config_data(observation_batch_mode=True, observation_max_batch_size=2, event_url=SECRET_URL)
    integration = make_integration(deliver=data, auth=AUTH_CONFIG_DATA)
    config_manager = mocker.MagicMock()
    config_manager.get_integration_details.return_value = async_return(integration)
    config_manager.get_action_configuration.side_effect = lambda integration_id, action_id: async_return(
        integration.get_action_config(action_id)
    )
    mocker.patch("app.services.action_runner.config_manager", config_manager)
    runner_events = mocker.patch("app.services.action_runner.publish_event", mocker.AsyncMock())
    return outbound_env, config_manager, runner_events


def _push(envelope):
    return api_client.post("/push-data", json={
        "message": {
            "data": base64.b64encode(envelope.json().encode()).decode(),
            "attributes": {"destination_id": INTEGRATION_ID},
        },
        "subscription": "projects/p/subscriptions/s",
    })


def test_outbound_actions_are_discovered():
    assert set(get_actions()) == {"auth", "deliver", "deliver_batch", "get_data_samples"}


def test_gundi_delivery_routes_to_deliver(runner):
    (send_json, _), _, _ = runner

    response = _push(GundiDelivery(payload=event(1), provider=PROVIDER))

    assert response.status_code == 200
    assert response.json() == {"delivered": True, "output_type": "event"}
    assert send_json.call_args.kwargs["url"] == SECRET_URL


def test_gundi_batch_delivery_routes_to_deliver_batch_without_its_own_config(runner):
    (send_json, _), config_manager, _ = runner
    bundle = GundiBatchDelivery(batch_id="batch-1", provider=PROVIDER, payloads=[observation(n) for n in range(4)] + [event(0)])

    response = _push(bundle)

    assert response.status_code == 200
    assert response.json()["delivered"]["observation"]["sent"] == 2
    assert send_json.call_count == 3
    # Internal action: the runner never looks for a stored "deliver_batch" row.
    assert all(call.args[1] != "deliver_batch" for call in config_manager.get_action_configuration.call_args_list)


def test_retryable_failure_answers_non_2xx_so_pubsub_redelivers(runner):
    (send_json, _), _, _ = runner
    send_json.side_effect = client.EndpointServerError("POST to hooks.example.com answered HTTP 503", 503)

    response = _push(GundiBatchDelivery(batch_id="batch-1", provider=PROVIDER, payloads=[event(0)]))

    assert response.status_code == 500
    assert response.json()["detail"]["retryable"] is True


def test_runner_failure_events_do_not_carry_the_endpoint_url(runner, published_events):
    (send_json, _), _, runner_events = runner
    send_json.side_effect = client.EndpointServerError("POST to hooks.slack.com answered HTTP 503", 503)

    response = _push(GundiDelivery(payload=event(1), provider=PROVIDER))

    published = json.dumps(
        [c.kwargs["event"].dict() for c in runner_events.call_args_list + published_events.call_args_list],
        default=str,
    ) + response.text
    assert runner_events.called and published_events.called
    assert "XXXXSECRET" not in published and "token=abc" not in published


@pytest.mark.asyncio
async def test_registration_shows_auth_and_one_deliver_form(mocker):
    gundi_client = mocker.MagicMock()
    gundi_client.register_integration_type = mocker.AsyncMock()
    mocker.patch("app.services.self_registration.INTEGRATION_TYPE_SLUG", "generic_webhook")

    await register_integration_in_gundi(gundi_client=gundi_client)

    actions = {a["value"]: a for a in gundi_client.register_integration_type.call_args.args[0]["actions"]}
    assert set(actions) == {"auth", "deliver", "get_data_samples"}
    assert (actions["auth"]["type"], actions["deliver"]["type"]) == ("auth", "push")
    # The portal hides reference actions from the configuration sections.
    assert actions["get_data_samples"]["type"] == "reference"
    # No pull action: the type must not look like a data source, and nothing runs on a schedule.
    assert not any(action["is_periodic_action"] for action in actions.values())
    # No always-green "Test Connection".
    assert "is_executable" not in actions["auth"]["schema"]


def test_get_data_samples_through_the_execute_endpoint(runner):
    # cdip's POST /v2/integrations/{id}/actions/get_data_samples/execute/ forwards
    # here and returns this body unchanged, so the portal reads body["samples"].
    _, config_manager, runner_events = runner
    _push(GundiDelivery(payload=event(1), provider=PROVIDER))  # capture is off: nothing stored

    response = api_client.post("/v1/actions/execute", json={
        "integration_id": INTEGRATION_ID, "action_id": "get_data_samples",
        "config_overrides": {"output_type": "event"},
    })

    assert response.status_code == 200
    assert response.json() == {
        "samples": {"event": []},
        "capture_enabled": {"observation": False, "event": False, "event_update": False, "message": False},
        "max_samples": 3,
        "ttl_seconds": 172800,
    }
    # Reference action: no stored row is looked up for it.
    assert all(call.args[1] != "get_data_samples" for call in config_manager.get_action_configuration.call_args_list)


def test_captured_samples_are_returned_by_the_execute_endpoint(mocker, outbound_env):
    data = deliver_config_data(event_capture_samples=True)
    integration = make_integration(deliver=data, auth=AUTH_CONFIG_DATA)
    config_manager = mocker.MagicMock()
    config_manager.get_integration_details.return_value = async_return(integration)
    config_manager.get_action_configuration.side_effect = lambda integration_id, action_id: async_return(
        integration.get_action_config(action_id)
    )
    mocker.patch("app.services.action_runner.config_manager", config_manager)
    mocker.patch("app.services.action_runner.publish_event", mocker.AsyncMock())
    _push(GundiDelivery(payload=event(1), provider=PROVIDER))
    _push(GundiDelivery(payload=event(2), provider=PROVIDER))

    body = api_client.post("/v1/actions/execute", json={
        "integration_id": INTEGRATION_ID, "action_id": "get_data_samples",
    }).json()

    assert set(body["samples"]) == {"observation", "event", "event_update", "message"}
    assert [s["record"]["title"] for s in body["samples"]["event"]] == ["Sighting 2", "Sighting 1"]
    assert body["samples"]["observation"] == []
    assert body["capture_enabled"]["event"] is True


def test_get_data_samples_for_a_draft_integration(mocker, outbound_env, fake_redis):
    # A draft run gets a fresh integration id, so no saved integration's samples can leak into it.
    mocker.patch("app.services.action_runner.publish_event", mocker.AsyncMock())
    fake_redis.lists[f"outbound_samples.{INTEGRATION_ID}.event"] = [b'{"captured_at": "x", "record": {}}']

    response = api_client.post("/v1/actions/execute", json={
        "action_id": "get_data_samples",
        "integration_state": {
            "type_value": "generic_webhook",
            "configurations": [{"action_value": "deliver", "data": deliver_config_data(event_capture_samples=True)}],
        },
    })

    assert response.status_code == 200
    body = response.json()
    assert body["samples"] == {"observation": [], "event": [], "event_update": [], "message": []}
    assert body["capture_enabled"] == {"observation": False, "event": True, "event_update": False, "message": False}
    assert fake_redis.lists[f"outbound_samples.{INTEGRATION_ID}.event"]


def test_get_data_samples_rejects_an_unknown_data_type(runner):
    response = api_client.post("/v1/actions/execute", json={
        "integration_id": INTEGRATION_ID, "action_id": "get_data_samples",
        "config_overrides": {"output_type": "bogus"},
    })

    assert response.status_code == 422
    assert "output_type" in response.text
