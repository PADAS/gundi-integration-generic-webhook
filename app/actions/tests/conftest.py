import datetime
import uuid

import pytest
from gundi_core.schemas.v2 import Integration

from app.actions import handlers
from app.services.batch_progress import BatchProgressStore
from app.services.outbound_buffer import OutboundBuffer
from app.services.tests.fake_redis import FakeRedis

INTEGRATION_ID = "0f2b3c1e-6a6e-4d36-9d59-6f3c1e2b9a10"
PROVIDER_ID = "ddd0946d-15b0-4308-b93d-e0470b6d33b6"
PUBLIC_ADDRESS = "93.184.216.34"


def _configuration(action_value, data):
    return {
        "id": str(uuid.uuid4()),
        "integration": INTEGRATION_ID,
        "action": {"id": str(uuid.uuid4()), "type": "generic", "name": action_value, "value": action_value},
        "data": data,
    }


def make_integration(deliver=None, auth=None):
    configurations = []
    if auth is not None:
        configurations.append(_configuration("auth", auth))
    if deliver is not None:
        configurations.append(_configuration("deliver", deliver))
    return Integration.parse_obj({
        "id": INTEGRATION_ID,
        "name": "Outbound Webhooks",
        "base_url": "",
        "enabled": True,
        "type": {
            "id": str(uuid.uuid4()), "name": "Generic Webhook", "value": "generic_webhook",
            "description": "", "actions": [],
        },
        "owner": {"id": str(uuid.uuid4()), "name": "Test Org", "description": ""},
        "configurations": configurations,
        "additional": {},
        "default_route": None,
        "status": "healthy",
        "status_details": "",
    })


DEFAULT_URLS = {
    "observation": "https://hooks.example.com/observations",
    "event": "https://hooks.example.com/events",
    "event_update": "https://hooks.example.com/event_update",
    "message": "https://hooks.example.com/message",
}
_ENDPOINT_FIELDS = ("url", "method", "jq_filter", "batch_mode", "max_batch_size", "max_wait_seconds")


def deliver_config_data(output_types=("observation", "event"), **overrides):
    """Deliver config data with one endpoint per type in output_types.

    Overrides are named `<type>_<field>` (e.g. event_batch_mode=True); one that
    names no configured endpoint field raises, so a typo cannot pass silently.
    """
    endpoints, used = [], set()
    for output_type in output_types:
        endpoint = {"output_type": output_type, "url": DEFAULT_URLS[output_type]}
        for field in _ENDPOINT_FIELDS:
            key = f"{output_type}_{field}"
            if key in overrides:
                endpoint[field] = overrides[key]
                used.add(key)
        endpoints.append(endpoint)
    if unused := set(overrides) - used:
        raise TypeError(f"Overrides for no configured endpoint field: {sorted(unused)}")
    return {"endpoints": endpoints}


AUTH_CONFIG_DATA = {"api_key": "s3cret", "custom_headers": [{"name": "X-Tenant", "value": "acme"}]}


def observation(n=0, **extra):
    return {
        "gundi_id": f"00000000-0000-0000-0000-{n:012d}",
        "source_name": f"Collar {n}",
        "external_source_id": f"collar-{n}",
        "recorded_at": datetime.datetime(2026, 10, 8, 12, n % 60, tzinfo=datetime.timezone.utc).isoformat(),
        "location": {"lat": -1.5, "lon": 36.8},
        "observation_type": "obv",
        **extra,
    }


def event(n=0):
    return {
        "gundi_id": f"10000000-0000-0000-0000-{n:012d}",
        "title": f"Sighting {n}",
        "event_type": "wildlife_sighting",
        "recorded_at": "2026-10-08T12:00:00+00:00",
        "location": {"lat": -1.5, "lon": 36.8},
        "observation_type": "ev",
    }


def text_message(n=0):
    return {
        "gundi_id": f"20000000-0000-0000-0000-{n:012d}",
        "sender": "ranger-1",
        "text": "hello",
        "created_at": "2026-10-08T12:00:00+00:00",
        "observation_type": "txt",
    }


def attachment(n=0):
    return {"gundi_id": f"30000000-0000-0000-0000-{n:012d}", "file_path": "a/b.jpg", "observation_type": "att"}


PROVIDER = {"provider_id": PROVIDER_ID, "provider_type": "collars", "provider_name": "Collars"}


@pytest.fixture
def fake_redis():
    return FakeRedis()


@pytest.fixture
def published_events(mocker):
    """Events the @activity_logger decorator publishes."""
    return mocker.patch("app.services.activity_logger.publish_event", mocker.AsyncMock())


@pytest.fixture
def outbound_env(mocker, fake_redis, published_events):
    """Handlers wired to an in-memory Redis, a mocked endpoint and a captured activity feed."""
    mocker.patch.object(handlers, "outbound_buffer", OutboundBuffer(db_client=fake_redis))
    mocker.patch.object(handlers, "batch_progress", BatchProgressStore(db_client=fake_redis))
    state_manager = mocker.MagicMock()
    state_manager.set_if_absent = mocker.AsyncMock(side_effect=[True, False, False])
    state_manager.delete_state = mocker.AsyncMock()
    mocker.patch.object(handlers, "state_manager", state_manager)
    mocker.patch("app.services.url_policy._resolve_addresses", mocker.AsyncMock(return_value=[PUBLIC_ADDRESS]))
    send_json = mocker.patch("app.actions.client.send_json", mocker.AsyncMock())
    log_activity = mocker.patch.object(handlers, "log_action_activity", mocker.AsyncMock())
    return send_json, log_activity
