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
    assert set(get_actions()) == {"auth", "deliver", "deliver_batch"}


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
    assert set(actions) == {"auth", "deliver"}
    assert (actions["auth"]["type"], actions["deliver"]["type"]) == ("auth", "push")
    # No pull action: the type must not look like a data source, and nothing runs on a schedule.
    assert not any(action["is_periodic_action"] for action in actions.values())
    # No always-green "Test Connection".
    assert "is_executable" not in actions["auth"]["schema"]
