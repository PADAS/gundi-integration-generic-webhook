import re
from enum import Enum
from typing import List, Optional

import pydantic

from app.services.utils import FieldWithUIOptions, GlobalUISchemaOptions, UIOptions
from .core import (
    AuthActionConfiguration,
    ExecutableActionMixin,
    InternalActionConfiguration,
    PullActionConfiguration,
    PushActionConfiguration,
    StoredConfigOptionalMixin,
)

# RFC 9110 field-name token.
_HEADER_NAME = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")


def _validate_header_name(name: str) -> str:
    name = name.strip()
    if not _HEADER_NAME.match(name):
        raise ValueError("must be a valid HTTP header name (letters, digits and -, no spaces or colons)")
    return name


def _clean_header_value(value, what: str) -> Optional[pydantic.SecretStr]:
    """Strip a header value and refuse what an HTTP client cannot send.

    Rejected at save time because at send time the same value would fail every
    request. Messages never echo the value: it is usually a secret.
    """
    if value is None:
        return None
    raw = value.get_secret_value() if isinstance(value, pydantic.SecretStr) else str(value)
    raw = raw.strip()
    if any((ord(c) < 32 and c != "\t") or ord(c) == 127 for c in raw):
        raise ValueError(f"{what} must not contain line breaks or other control characters")
    # httpx encodes header values as ASCII.
    if not raw.isascii():
        raise ValueError(f"{what} may only contain ASCII characters (no accents, emoji or symbols such as é or ✓)")
    return pydantic.SecretStr(raw)


class CustomHeader(pydantic.BaseModel):
    name: str = pydantic.Field(..., title="Header Name", example="X-Api-Version")
    value: pydantic.SecretStr = FieldWithUIOptions(
        ...,
        title="Header Value",
        ui_options=UIOptions(widget="password"),
    )

    _check_name = pydantic.validator("name", allow_reuse=True)(_validate_header_name)

    @pydantic.validator("value")
    def clean_value(cls, v):
        return _clean_header_value(v, "Header Value")


class AuthenticateConfig(AuthActionConfiguration, ExecutableActionMixin):
    api_key: Optional[pydantic.SecretStr] = FieldWithUIOptions(
        None,
        title="API Key",
        description="Sent on every request in the header below. Leave empty if the endpoint needs no key.",
        ui_options=UIOptions(widget="password"),
    )
    api_key_header: str = FieldWithUIOptions(
        "Authorization",
        title="API Key Header",
        description="Name of the header that carries the API key.",
    )
    api_key_prefix: str = FieldWithUIOptions(
        "Bearer",
        title="API Key Prefix",
        description="Written before the key, separated by a space (e.g. 'Bearer', 'Token'). Leave empty to send the raw key.",
    )
    custom_headers: List[CustomHeader] = FieldWithUIOptions(
        [],
        title="Custom Headers",
        description="Extra headers sent on every request. Values are stored as secrets.",
    )

    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=["api_key", "api_key_header", "api_key_prefix", "custom_headers"],
    )

    _check_header = pydantic.validator("api_key_header", allow_reuse=True)(_validate_header_name)

    @pydantic.validator("api_key")
    def clean_api_key(cls, v):
        v = _clean_header_value(v, "API Key")
        return v if v and v.get_secret_value() else None

    @pydantic.validator("api_key_prefix")
    def clean_prefix(cls, v):
        return _clean_header_value(v, "API Key Prefix").get_secret_value()

    @classmethod
    def ui_schema(cls, *args, **kwargs):
        base = super().ui_schema(*args, **kwargs)
        # Nested objects lose their key order in the portal's jsonb storage.
        base["custom_headers"] = {
            "items": {
                "ui:order": list(CustomHeader.__fields__),
                "value": {"ui:widget": "password"},
            },
        }
        return base


class OutputType(str, Enum):
    OBSERVATION = "observation"
    EVENT = "event"
    EVENT_UPDATE = "event_update"
    MESSAGE = "message"


OUTPUT_TYPE_TITLES = {
    OutputType.OBSERVATION: "Observations",
    OutputType.EVENT: "Events",
    OutputType.EVENT_UPDATE: "Event Updates",
    OutputType.MESSAGE: "Messages",
}


class HttpMethod(str, Enum):
    POST = "POST"
    PUT = "PUT"


class EndpointSettings(pydantic.BaseModel):
    """Where and how one output type is delivered, read off DeliverConfig's flat fields."""
    output_type: OutputType
    url: Optional[str]  # None when the portal saved a selected type without one
    method: HttpMethod
    jq_filter: str
    batch_mode: bool
    max_batch_size: int
    max_wait_seconds: int


def _url_field(t: OutputType):
    # Secret because webhook URLs often are credentials (Slack-style hook
    # paths, ?token=...), and this keeps them out of activity-log config data.
    return FieldWithUIOptions(
        None,
        title=f"{OUTPUT_TYPE_TITLES[t]}: URL",
        description=(
            "HTTPS endpoint that receives this data type. Required when the type is selected above: "
            "without it, records of this type are dropped and an error is logged. Records buffered "
            "while the type was selected are still sent here after it is deselected, if a URL is set."
        ),
        ui_options=UIOptions(widget="password", placeholder="https://example.com/webhooks/gundi"),
    )


def _method_field(t: OutputType):
    return pydantic.Field(HttpMethod.POST, title=f"{OUTPUT_TYPE_TITLES[t]}: HTTP Method")


def _jq_field(t: OutputType):
    return FieldWithUIOptions(
        ".",
        title=f"{OUTPUT_TYPE_TITLES[t]}: JQ Filter",
        description=(
            "Shapes the request body. Input is one record, or the array of records in batch mode. "
            "No output (or null) skips the request, one output is the body, several are sent as a JSON array. "
            "In batch mode, a filter that fails on any record drops the whole batch (logged as an error)."
        ),
        ui_options=UIOptions(widget="textarea", rows=4),
    )


def _batch_mode_field(t: OutputType):
    return pydantic.Field(
        False,
        title=f"{OUTPUT_TYPE_TITLES[t]}: Batch Mode",
        description="Send records in groups instead of one request per record.",
    )


def _max_batch_size_field(t: OutputType):
    return pydantic.Field(
        100, ge=1,
        title=f"{OUTPUT_TYPE_TITLES[t]}: Max Batch Size",
        description="Batch mode: most records sent in one request.",
    )


def _max_wait_field(t: OutputType):
    # Buffers are flushed by a once-a-minute schedule, so a shorter wait could not be honoured.
    return pydantic.Field(
        60, ge=60,
        title=f"{OUTPUT_TYPE_TITLES[t]}: Max Wait (seconds)",
        description="Batch mode: a partial batch is sent once its oldest record has waited this long.",
    )


_ENDPOINT_FIELDS = ("url", "method", "jq_filter", "batch_mode", "max_batch_size", "max_wait_seconds")


class DeliverConfig(PushActionConfiguration):
    output_types: List[OutputType] = FieldWithUIOptions(
        ...,
        min_items=1,
        unique_items=True,
        title="Data Types to Deliver",
        description="Data types sent to the endpoints below. Anything else routed to this integration is dropped.",
        ui_options=UIOptions(widget="checkboxes"),
    )

    observation_url: Optional[pydantic.SecretStr] = _url_field(OutputType.OBSERVATION)
    observation_method: HttpMethod = _method_field(OutputType.OBSERVATION)
    observation_jq_filter: str = _jq_field(OutputType.OBSERVATION)
    observation_batch_mode: bool = _batch_mode_field(OutputType.OBSERVATION)
    observation_max_batch_size: int = _max_batch_size_field(OutputType.OBSERVATION)
    observation_max_wait_seconds: int = _max_wait_field(OutputType.OBSERVATION)

    event_url: Optional[pydantic.SecretStr] = _url_field(OutputType.EVENT)
    event_method: HttpMethod = _method_field(OutputType.EVENT)
    event_jq_filter: str = _jq_field(OutputType.EVENT)
    event_batch_mode: bool = _batch_mode_field(OutputType.EVENT)
    event_max_batch_size: int = _max_batch_size_field(OutputType.EVENT)
    event_max_wait_seconds: int = _max_wait_field(OutputType.EVENT)

    event_update_url: Optional[pydantic.SecretStr] = _url_field(OutputType.EVENT_UPDATE)
    event_update_method: HttpMethod = _method_field(OutputType.EVENT_UPDATE)
    event_update_jq_filter: str = _jq_field(OutputType.EVENT_UPDATE)
    event_update_batch_mode: bool = _batch_mode_field(OutputType.EVENT_UPDATE)
    event_update_max_batch_size: int = _max_batch_size_field(OutputType.EVENT_UPDATE)
    event_update_max_wait_seconds: int = _max_wait_field(OutputType.EVENT_UPDATE)

    message_url: Optional[pydantic.SecretStr] = _url_field(OutputType.MESSAGE)
    message_method: HttpMethod = _method_field(OutputType.MESSAGE)
    message_jq_filter: str = _jq_field(OutputType.MESSAGE)
    message_batch_mode: bool = _batch_mode_field(OutputType.MESSAGE)
    message_max_batch_size: int = _max_batch_size_field(OutputType.MESSAGE)
    message_max_wait_seconds: int = _max_wait_field(OutputType.MESSAGE)

    # A missing URL for a selected type is not a validation error: the portal
    # form cannot enforce it, and a config that fails to parse fails every
    # message routed here. Delivery drops such records with an error instead.
    @pydantic.validator("observation_url", "event_url", "event_update_url", "message_url", pre=True)
    def blank_url_is_unset(cls, v):
        if isinstance(v, pydantic.SecretStr):
            v = v.get_secret_value()
        return (v or "").strip() or None

    @classmethod
    def schema(cls, **kwargs):
        schema = super().schema(**kwargs)
        # Checkbox labels: rjsf reads options from `oneOf` const/title.
        schema["properties"]["output_types"]["items"] = {
            "type": "string",
            "oneOf": [{"const": t.value, "title": OUTPUT_TYPE_TITLES[t]} for t in OutputType],
        }
        schema.get("definitions", {}).pop("OutputType", None)
        return schema

    def endpoint_for(self, output_type: OutputType) -> EndpointSettings:
        prefix = OutputType(output_type).value
        values = {name: getattr(self, f"{prefix}_{name}") for name in _ENDPOINT_FIELDS}
        values["url"] = values["url"].get_secret_value() if values["url"] else None
        return EndpointSettings(output_type=output_type, **values)

    @classmethod
    def ui_schema(cls, *args, **kwargs):
        base = super().ui_schema(*args, **kwargs)
        # The portal stores schemas in jsonb, which reorders object keys, so the
        # form follows this list rather than the field declaration order.
        base["ui:order"] = ["output_types"] + [
            f"{t.value}_{name}" for t in OutputType for name in _ENDPOINT_FIELDS
        ]
        return base


class DeliverBatchConfig(PushActionConfiguration, InternalActionConfiguration):
    """Marker for action_deliver_batch, which runs with the stored `deliver` config.

    Push actions are discovered by their PushActionConfiguration annotation,
    so it is needed to route GundiBatchDelivery here. Being internal keeps it
    out of registration (no second portal form) and lets the runner execute it
    without a stored config row, which would otherwise answer 404 and leave
    Pub/Sub redelivering the bundle.
    """


class FlushBuffersConfig(StoredConfigOptionalMixin, PullActionConfiguration):
    """No fields of its own: what to flush follows from the `deliver` config,
    so it runs for every integration without a saved Flush Buffers form."""
