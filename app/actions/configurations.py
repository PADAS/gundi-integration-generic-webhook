import re
from enum import Enum
from typing import List, Optional

import pydantic

from app import settings
from app.services.utils import FieldWithUIOptions, GlobalUISchemaOptions, UIOptions
from .core import (
    AuthActionConfiguration,
    InternalActionConfiguration,
    PushActionConfiguration,
    ReferenceActionConfiguration,
)
from .jq_editor import jq_transform_annotation

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


class AuthenticateConfig(AuthActionConfiguration):
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
    """An Endpoint as the handlers use it: the URL unwrapped."""
    output_type: OutputType
    url: str
    method: HttpMethod
    jq_filter: str
    batch_mode: bool
    max_batch_size: int
    max_wait_seconds: int
    capture_samples: bool


MAX_BATCH_SIZE = 10000


def duration_text(seconds: int) -> str:
    """'2 days', '36 hours', '90 minutes': the largest unit that divides evenly."""
    for unit, size in (("day", 86400), ("hour", 3600), ("minute", 60), ("second", 1)):
        if seconds % size == 0:
            count = seconds // size
            return f"{count} {unit}{'' if count == 1 else 's'}"


def _capture_samples_description() -> str:
    ttl = duration_text(settings.OUTBOUND_SAMPLES_TTL_SECONDS)
    return (
        f"While on, the latest {settings.OUTBOUND_SAMPLES_MAX} records of this data type, as the JQ filter "
        f"receives them, are kept for {ttl} for use in the transformation editor. They are real data. "
        f"Turning this off hides them at once; they are deleted when the next record of this type arrives, "
        f"or within {ttl}."
    )

_OUTPUT_TYPE_ONE_OF = [{"const": t.value, "title": OUTPUT_TYPE_TITLES[t]} for t in OutputType]
_URL_PATTERN = "^https://"


class Endpoint(pydantic.BaseModel):
    output_type: OutputType = pydantic.Field(..., title="Data Type")
    # SecretStr because webhook URLs are often credentials (Slack-style hook
    # paths, ?token=...): it keeps them out of activity-log config data and
    # error events. The portal still shows it as plain text (see Config below)
    # so users can read back what they saved. The pattern lets the portal and
    # cdip refuse a non-https URL on save.
    url: pydantic.SecretStr = FieldWithUIOptions(
        ...,
        title="URL",
        description="HTTPS endpoint that receives this data type.",
        pattern=_URL_PATTERN,
        ui_options=UIOptions(widget="text", placeholder="https://example.com/webhooks/gundi"),
    )
    method: HttpMethod = pydantic.Field(HttpMethod.POST, title="HTTP Method")
    # Its ui schema is set in DeliverConfig.ui_schema.
    jq_filter: str = pydantic.Field(
        ".",
        title="JQ Filter",
        description=(
            "Shapes the request body. Input: one record, or an array of records in batch mode. "
            "No output (or null) skips the request; several outputs are sent as a JSON array. "
            "In batch mode, a filter that fails on any record drops the whole batch."
        ),
    )
    batch_mode: bool = pydantic.Field(
        False, title="Batch Mode", description="Send records in groups instead of one request per record.",
    )
    # The buffer's default cap (OUTBOUND_BUFFER_MAX_RECORDS) is 10000, so a
    # larger batch could never fill. It also bounds one request's size.
    max_batch_size: int = pydantic.Field(
        100, ge=1, le=MAX_BATCH_SIZE, title="Max Batch Size",
        description=f"Batch mode: most records in one request (1 to {MAX_BATCH_SIZE}).",
    )
    max_wait_seconds: int = pydantic.Field(
        60, ge=60, title="Max Wait (seconds)",
        description=(
            "Batch mode: a partial batch is sent with the next record that arrives for this integration "
            "after its oldest record has waited this long."
        ),
    )

    capture_samples: bool = pydantic.Field(
        False, title="Capture data samples", description=_capture_samples_description(),
    )

    @pydantic.validator("url", pre=True)
    def https_url(cls, v):
        raw = v.get_secret_value() if isinstance(v, pydantic.SecretStr) else str(v or "")
        raw = raw.strip()
        if not raw.startswith("https://"):
            raise ValueError("must start with https://")
        return raw

    def settings(self) -> EndpointSettings:
        return EndpointSettings(**{**self.dict(), "url": self.url.get_secret_value()})

    class Config:
        @staticmethod
        def schema_extra(schema, model):
            # SecretStr emits format=password/writeOnly, which makes forms mask
            # the URL; it's shown as text, and redaction keys off the type.
            schema["properties"]["url"].pop("format", None)
            schema["properties"]["url"].pop("writeOnly", None)


class DeliverConfig(PushActionConfiguration):
    endpoints: List[Endpoint] = pydantic.Field(
        [],
        title="Endpoints",
        description="One per data type to deliver. Data types without an endpoint are dropped.",
    )

    @pydantic.validator("endpoints")
    def one_endpoint_per_type(cls, v):
        seen = set()
        for endpoint in v:
            if endpoint.output_type in seen:
                raise ValueError(
                    f"only one endpoint per data type; {OUTPUT_TYPE_TITLES[endpoint.output_type]} appears more than once"
                )
            seen.add(endpoint.output_type)
        return v

    @property
    def output_types(self) -> List[OutputType]:
        return [endpoint.output_type for endpoint in self.endpoints]

    def endpoint_for(self, output_type: OutputType) -> Optional[EndpointSettings]:
        """The type's endpoint, or None when the type is not delivered."""
        for endpoint in self.endpoints:
            if endpoint.output_type == output_type:
                return endpoint.settings()
        return None

    @classmethod
    def schema(cls, **kwargs):
        schema = super().schema(**kwargs)
        definitions = schema.get("definitions", {})
        # Readable select options: rjsf reads them from `oneOf` const/title.
        definitions["Endpoint"]["properties"]["output_type"] = {
            "title": "Data Type", "type": "string", "oneOf": _OUTPUT_TYPE_ONE_OF,
        }
        definitions.pop("OutputType", None)
        return schema

    @classmethod
    def ui_schema(cls, *args, **kwargs):
        base = super().ui_schema(*args, **kwargs)
        # Nested objects lose their key order in the portal's jsonb storage, so
        # array items carry their own ui:order (as custom_headers does).
        endpoint_fields = Endpoint.__fields__
        base["ui:order"] = ["endpoints"]
        base["endpoints"] = {
            "items": {
                "ui:order": list(endpoint_fields),
                **{name: field.field_info.ui_schema() for name, field in endpoint_fields.items()
                   if getattr(field.field_info, "ui_options", None)},
            },
        }
        # The portal's jq editor keys off ui:options.language on a textarea. A
        # "jq" widget name would make rjsf throw in portals that lack it.
        base["endpoints"]["items"]["jq_filter"] = {
            "ui:widget": "textarea",
            "ui:options": {"language": "jq", "rows": 4},
            "gundi:jq_transform": jq_transform_annotation(
                output_type_field="output_type", batch_field="batch_mode", samples_action="get_data_samples",
            ),
        }
        return base


class DeliverBatchConfig(PushActionConfiguration, InternalActionConfiguration):
    """Marker for action_deliver_batch, which runs with the stored `deliver` config.

    Push actions are discovered by their PushActionConfiguration annotation,
    so it is needed to route GundiBatchDelivery here. Being internal keeps it
    out of registration (no second portal form) and lets the runner execute it
    without a stored config row, which would otherwise answer 404 and leave
    Pub/Sub redelivering the bundle.
    """


class GetDataSamplesQuery(ReferenceActionConfiguration):
    """Recent records captured for the transformation editor (see Endpoint.capture_samples)."""

    output_type: Optional[OutputType] = pydantic.Field(
        None, title="Data Type", description="Only this data type's samples. Leave unset to return every type.",
    )
