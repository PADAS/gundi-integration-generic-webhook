import json

import pydantic
import pytest

from app.actions.configurations import AuthenticateConfig, DeliverConfig, HttpMethod, OutputType
from app.actions.tests.conftest import deliver_config_data
from app.services.redaction import REDACTED, redact_secrets


def test_endpoint_for_reads_the_type_specific_fields():
    config = DeliverConfig.parse_obj(deliver_config_data(
        event_method="PUT", event_jq_filter="{t: .title}", event_batch_mode=True,
        event_max_batch_size=20, event_max_wait_seconds=300,
    ))

    endpoint = config.endpoint_for(OutputType.EVENT)

    assert endpoint.output_type == OutputType.EVENT
    assert endpoint.url == "https://hooks.example.com/events"
    assert endpoint.method == HttpMethod.PUT
    assert endpoint.jq_filter == "{t: .title}"
    assert (endpoint.batch_mode, endpoint.max_batch_size, endpoint.max_wait_seconds) == (True, 20, 300)
    observation_endpoint = config.endpoint_for(OutputType.OBSERVATION)
    assert (observation_endpoint.method, observation_endpoint.jq_filter, observation_endpoint.batch_mode) == (
        HttpMethod.POST, ".", False,
    )


def test_a_selected_type_without_a_url_still_parses():
    # The portal cannot enforce it, and a config that fails to parse would fail
    # every message; delivery drops those records with an error instead.
    config = DeliverConfig.parse_obj(deliver_config_data(output_types=["observation", "message"]))
    assert config.endpoint_for(OutputType.MESSAGE).url is None


def test_a_blank_url_counts_as_missing():
    config = DeliverConfig.parse_obj(deliver_config_data(event_url="  "))
    assert config.event_url is None
    assert config.endpoint_for(OutputType.EVENT).url is None


def test_urls_are_secrets_unwrapped_only_for_delivery():
    url = "https://hooks.slack.com/services/T0/B0/XXXXSECRET?token=abc"
    config = DeliverConfig.parse_obj(deliver_config_data(event_url=f" {url} "))

    assert config.endpoint_for(OutputType.EVENT).url == url
    assert "XXXXSECRET" not in repr(config) and "XXXXSECRET" not in json.dumps(config.dict(), default=str)
    assert redact_secrets(config.dict(), model=DeliverConfig)["event_url"] == REDACTED
    assert DeliverConfig.ui_schema()["event_url"]["ui:widget"] == "password"


def test_unselected_types_need_no_url():
    config = DeliverConfig.parse_obj({"output_types": ["message"], "message_url": "https://x.example.com/m"})
    assert config.observation_url is None


@pytest.mark.parametrize("overrides", [
    {"output_types": []},
    {"output_types": ["observation", "observation"]},
    {"output_types": ["attachment"]},
    {"observation_max_batch_size": 0},
    {"observation_max_wait_seconds": 59},
    {"observation_method": "DELETE"},
])
def test_invalid_values_are_rejected(overrides):
    with pytest.raises(pydantic.ValidationError):
        DeliverConfig.parse_obj(deliver_config_data(**overrides))


def test_deliver_ui_schema_orders_every_field_and_renders_checkboxes():
    ui_schema = DeliverConfig.ui_schema()
    schema = json.loads(DeliverConfig.schema_json())

    assert ui_schema["output_types"] == {"ui:widget": "checkboxes"}
    assert ui_schema["event_jq_filter"]["ui:widget"] == "textarea"
    assert sorted(ui_schema["ui:order"]) == sorted(schema["properties"])
    assert ui_schema["ui:order"][:3] == ["output_types", "observation_url", "observation_method"]
    assert schema["properties"]["output_types"]["uniqueItems"] is True


def test_output_type_checkboxes_have_readable_labels():
    items = json.loads(DeliverConfig.schema_json())["properties"]["output_types"]["items"]
    assert items["oneOf"] == [
        {"const": "observation", "title": "Observations"},
        {"const": "event", "title": "Events"},
        {"const": "event_update", "title": "Event Updates"},
        {"const": "message", "title": "Messages"},
    ]


def test_auth_header_names_are_validated():
    with pytest.raises(pydantic.ValidationError):
        AuthenticateConfig(api_key_header="Bad Header")
    with pytest.raises(pydantic.ValidationError):
        AuthenticateConfig(custom_headers=[{"name": "X-A:", "value": "v"}])


@pytest.mark.parametrize("data", [
    {"api_key": "abc\r\nX-Injected: 1"},
    {"api_key": "caf\u00e9\u2713"},
    {"api_key_prefix": "Bea\nrer"},
    {"api_key_prefix": "B\u2713"},
    {"custom_headers": [{"name": "X-A", "value": "a\nb"}]},
    {"custom_headers": [{"name": "X-A", "value": "\u2713"}]},
    {"custom_headers": [{"name": "X-A", "value": "a\x00b"}]},
    {"custom_headers": [{"name": "X-A", "value": "caf\u00e9"}]},  # latin-1, but httpx sends ASCII
    {"api_key": "na\u00efve-token"},
])
def test_header_values_an_http_client_cannot_send_are_rejected(data):
    with pytest.raises(pydantic.ValidationError) as exc_info:
        AuthenticateConfig.parse_obj(data)
    # The value is usually a secret: the message must not echo it.
    assert "abc" not in str(exc_info.value) and "Injected" not in str(exc_info.value)


def test_header_values_are_stripped_and_a_blank_key_is_unset():
    config = AuthenticateConfig.parse_obj({
        "api_key": "  k  ", "api_key_prefix": " Token ", "custom_headers": [{"name": " X-A ", "value": " v1 "}],
    })
    assert config.api_key.get_secret_value() == "k"
    assert config.api_key_prefix == "Token"
    assert (config.custom_headers[0].name, config.custom_headers[0].value.get_secret_value()) == ("X-A", "v1")
    assert AuthenticateConfig(api_key="   ").api_key is None


def test_auth_secrets_are_masked_in_portal_and_logs():
    config = AuthenticateConfig.parse_obj({
        "api_key": "s3cret", "custom_headers": [{"name": "X-Tenant", "value": "acme"}],
    })
    ui_schema = AuthenticateConfig.ui_schema()

    assert ui_schema["api_key"] == {"ui:widget": "password"}
    assert ui_schema["custom_headers"]["items"]["value"] == {"ui:widget": "password"}
    redacted = redact_secrets(config.dict(), model=AuthenticateConfig)
    assert redacted["api_key"] == REDACTED
    assert redacted["custom_headers"][0] == {"name": "X-Tenant", "value": REDACTED}
    assert "s3cret" not in repr(config) and "acme" not in repr(config)
