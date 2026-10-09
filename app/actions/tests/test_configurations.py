import json
import re

import pydantic
import pytest

from app.actions.configurations import AuthenticateConfig, DeliverConfig, HttpMethod, OutputType
from app.actions.core import ExecutableActionMixin
from app.actions.tests.conftest import deliver_config_data
from app.services.redaction import REDACTED, redact_secrets


def test_endpoint_for_reads_the_types_endpoint():
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
    assert config.output_types == [OutputType.OBSERVATION, OutputType.EVENT]


def test_a_type_without_an_endpoint_is_not_delivered():
    config = DeliverConfig.parse_obj(deliver_config_data(output_types=["message"]))
    assert config.endpoint_for(OutputType.EVENT) is None


def test_an_empty_row_parses_and_delivers_nothing():
    # cdip creates a {} row for every action when an integration is created.
    assert DeliverConfig.parse_obj({}).endpoints == []


def test_only_one_endpoint_per_data_type():
    data = {"endpoints": [
        {"output_type": "event", "url": "https://a.example.com"},
        {"output_type": "event", "url": "https://b.example.com"},
    ]}
    with pytest.raises(pydantic.ValidationError, match="only one endpoint per data type; Events appears more than once"):
        DeliverConfig.parse_obj(data)


@pytest.mark.parametrize("url", ["http://hooks.example.com/x", "", "   ", "hooks.example.com", "ftp://x"])
def test_endpoint_urls_must_be_https(url):
    with pytest.raises(pydantic.ValidationError, match="must start with https://"):
        DeliverConfig.parse_obj({"endpoints": [{"output_type": "event", "url": url}]})


def test_the_https_rule_is_in_the_schema_so_the_portal_and_cdip_refuse_it_on_save():
    url_schema = json.loads(DeliverConfig.schema_json())["definitions"]["Endpoint"]["properties"]["url"]
    assert url_schema["pattern"] == "^https://"
    assert url_schema["format"] == "password"
    for url, accepted in [("https://x.example.com/h", True), ("http://x.example.com/h", False)]:
        assert bool(re.match(url_schema["pattern"], url)) is accepted
        try:
            DeliverConfig.parse_obj({"endpoints": [{"output_type": "event", "url": url}]})
            assert accepted
        except pydantic.ValidationError:
            assert not accepted


def test_urls_are_secrets_unwrapped_only_for_delivery():
    url = "https://hooks.slack.com/services/T0/B0/XXXXSECRET?token=abc"
    config = DeliverConfig.parse_obj(deliver_config_data(event_url=f" {url} "))

    assert config.endpoint_for(OutputType.EVENT).url == url
    assert "XXXXSECRET" not in repr(config) and "XXXXSECRET" not in json.dumps(config.dict(), default=str)
    assert redact_secrets(config.dict(), model=DeliverConfig)["endpoints"][1]["url"] == REDACTED


@pytest.mark.parametrize("endpoint", [
    {"output_type": "attachment", "url": "https://x.example.com"},
    {"output_type": "event"},
    {"output_type": "event", "url": "https://x.example.com", "max_batch_size": 0},
    {"output_type": "event", "url": "https://x.example.com", "max_wait_seconds": 59},
    {"output_type": "event", "url": "https://x.example.com", "method": "DELETE"},
])
def test_invalid_endpoints_are_rejected(endpoint):
    with pytest.raises(pydantic.ValidationError):
        DeliverConfig.parse_obj({"endpoints": [endpoint]})


def test_deliver_ui_schema_renders_endpoint_items():
    ui_schema = DeliverConfig.ui_schema()
    endpoint_schema = json.loads(DeliverConfig.schema_json())["definitions"]["Endpoint"]
    items = ui_schema["endpoints"]["items"]

    assert ui_schema["ui:order"] == ["endpoints"]
    # jsonb storage reorders keys, so the item order must be explicit and complete.
    assert sorted(items["ui:order"]) == sorted(endpoint_schema["properties"])
    assert items["ui:order"][:2] == ["output_type", "url"]
    assert items["url"] == {"ui:widget": "password", "ui:placeholder": "https://example.com/webhooks/gundi"}
    assert items["jq_filter"] == {"ui:widget": "textarea", "ui:options": {"language": "jq"}, "ui:rows": 4}


def test_data_type_options_have_readable_labels():
    schema = json.loads(DeliverConfig.schema_json())
    assert schema["definitions"]["Endpoint"]["properties"]["output_type"]["oneOf"] == [
        {"const": "observation", "title": "Observations"},
        {"const": "event", "title": "Events"},
        {"const": "event_update", "title": "Event Updates"},
        {"const": "message", "title": "Messages"},
    ]
    assert "OutputType" not in schema["definitions"]


def test_auth_has_no_test_connection_button():
    # The handler cannot probe arbitrary endpoints, so the portal would always say "Valid Credentials".
    assert not issubclass(AuthenticateConfig, ExecutableActionMixin)


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
