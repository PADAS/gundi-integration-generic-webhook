import json
import re

import pydantic
import pytest

from app import settings
from app.actions import configurations
from app.actions.configurations import AuthenticateConfig, DeliverConfig, Endpoint, HttpMethod, OutputType
from app.actions.jq_editor import MODELS
from app.actions.core import ExecutableActionMixin
from app.actions.tests.conftest import deliver_config_data
from app.services.jq_transform import jq_input
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
    # Shown as plain text in the portal; still a SecretStr for redaction.
    assert "format" not in url_schema and "writeOnly" not in url_schema
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
    assert items["url"] == {"ui:widget": "text", "ui:placeholder": "https://example.com/webhooks/gundi"}
    assert items["ui:order"][-1] == "capture_samples"
    jq_filter = dict(items["jq_filter"])
    annotation = jq_filter.pop("gundi:jq_transform")
    assert jq_filter == {"ui:widget": "textarea", "ui:options": {"language": "jq", "rows": 4}}
    assert {k: annotation[k] for k in ("output_type_field", "batch_field", "samples_action")} == {
        "output_type_field": "output_type", "batch_field": "batch_mode", "samples_action": "get_data_samples",
    }
    assert annotation["output_type_field"] in endpoint_schema["properties"]
    assert annotation["batch_field"] in endpoint_schema["properties"]


def _jq_annotation():
    return DeliverConfig.ui_schema()["endpoints"]["items"]["jq_filter"]["gundi:jq_transform"]


def test_jq_annotation_covers_every_output_type_with_gundi_core_schemas():
    annotation = _jq_annotation()
    types = {t.value for t in OutputType}
    assert set(MODELS) == set(annotation["schemas"]) == set(annotation["examples"]) == types
    for name, model in MODELS.items():
        assert annotation["schemas"][name] == model.schema()


@pytest.mark.parametrize("output_type", [t.value for t in OutputType])
def test_jq_examples_are_shaped_like_the_jq_input(output_type):
    example = _jq_annotation()["examples"][output_type]
    parsed = MODELS[output_type].parse_obj(example)
    assert jq_input(parsed) == example
    assert example["gundi_id"] and example["data_provider_id"]


def test_jq_examples_carry_extra_data_keys():
    examples = _jq_annotation()["examples"]
    assert len(examples["observation"]["additional"]) >= 2
    assert len(examples["event"]["event_details"]) >= 2
    assert len(examples["message"]["additional"]) >= 2
    assert examples["event_update"]["changes"]


def test_deliver_ui_schema_stays_small():
    # Stored per integration type and sent to the portal with every config form.
    assert len(json.dumps(DeliverConfig.ui_schema())) < 40 * 1024


def test_sample_capture_is_off_by_default_and_explains_itself():
    endpoint = Endpoint(output_type="event", url="https://x.example.com")
    assert endpoint.capture_samples is False
    assert endpoint.settings().capture_samples is False
    field = json.loads(DeliverConfig.schema_json())["definitions"]["Endpoint"]["properties"]["capture_samples"]
    assert field["title"] == "Capture data samples"
    assert field["description"] == (
        "While on, the latest 3 records of this data type, as the JQ filter receives them, are kept for "
        "2 days for use in the transformation editor. They are real data. Turning this off hides them at "
        "once; they are deleted when the next record of this type arrives, or within 2 days."
    )


def test_the_capture_description_follows_the_settings(mocker):
    mocker.patch.object(settings, "OUTBOUND_SAMPLES_MAX", 5)
    mocker.patch.object(settings, "OUTBOUND_SAMPLES_TTL_SECONDS", 36 * 3600)
    description = configurations._capture_samples_description()
    assert "the latest 5 records" in description and description.count("36 hours") == 2


@pytest.mark.parametrize("seconds, text", [
    (172800, "2 days"), (86400, "1 day"), (129600, "36 hours"), (5400, "90 minutes"), (61, "61 seconds"),
])
def test_duration_text(seconds, text):
    assert configurations.duration_text(seconds) == text


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


def test_max_batch_size_is_bounded_in_the_model_and_the_schema():
    def endpoint(size):
        return {"endpoints": [{"output_type": "event", "url": "https://x.example.com", "max_batch_size": size}]}

    assert DeliverConfig.parse_obj(endpoint(10000)).endpoint_for(OutputType.EVENT).max_batch_size == 10000
    with pytest.raises(pydantic.ValidationError):
        DeliverConfig.parse_obj(endpoint(10001))
    size_schema = json.loads(DeliverConfig.schema_json())["definitions"]["Endpoint"]["properties"]["max_batch_size"]
    assert (size_schema["minimum"], size_schema["maximum"]) == (1, 10000)
