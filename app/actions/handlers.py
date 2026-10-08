import json
import logging
from collections import Counter
from typing import Any, Dict, List, Optional

import pydantic

# app.settings before gundi_client_v2 (see app/services/errors.py).
from app import settings
from gundi_client_v2.transformations import apply_transformations
from gundi_core import schemas
from gundi_core.events import GundiDelivery
from gundi_core.schemas.v2 import Integration, LogLevel

from app.services.action_scheduler import crontab_schedule
from app.services.activity_logger import activity_logger, log_action_activity
from app.services.batch_progress import BatchProgressStore, decode, fingerprint
from app.services.errors import (
    IntegrationAuthError,
    IntegrationBadResponseError,
    IntegrationConfigurationError,
    IntegrationConnectionError,
    IntegrationError,
    IntegrationRateLimitError,
    format_error_message,
)
from app.services.jq_transform import outbound_body
from app.services.outbound_buffer import OutboundBuffer
from app.services.state import IntegrationStateManager
from app.services.url_policy import URLResolutionError, validate_outbound_url
from app.services.utils import generate_batches
from . import client
from .configurations import (
    OUTPUT_TYPE_TITLES,
    AuthenticateConfig,
    DeliverBatchConfig,
    DeliverConfig,
    EndpointSettings,
    FlushBuffersConfig,
    OutputType,
)
from .envelopes import GundiBatchDelivery

logger = logging.getLogger(__name__)

outbound_buffer = OutboundBuffer()
batch_progress = BatchProgressStore()
state_manager = IntegrationStateManager()

# flush_buffers runs every minute; an invalid deliver config is reported at most this often.
INVALID_CONFIG_WARNING_THROTTLE_SECONDS = 3600
# A full buffer drops a record per incoming record; the drops are reported at most this often.
BUFFER_OVERFLOW_REPORT_SECONDS = 600

_OUTPUT_TYPES_BY_PAYLOAD = {
    schemas.v2.Observation: OutputType.OBSERVATION,
    schemas.v2.Event: OutputType.EVENT,
    schemas.v2.EventUpdate: OutputType.EVENT_UPDATE,
    schemas.v2.TextMessage: OutputType.MESSAGE,
}

_INTEGRATION_ERRORS = {
    client.EndpointConnectionError: IntegrationConnectionError,
    client.EndpointRateLimitError: IntegrationRateLimitError,
    client.EndpointServerError: IntegrationBadResponseError,
    client.EndpointAuthError: IntegrationAuthError,
    client.EndpointRejectedError: IntegrationBadResponseError,
    client.EndpointRequestError: IntegrationConfigurationError,
}


def build_headers(auth_config: Optional[AuthenticateConfig]) -> Dict[str, str]:
    headers = {"Content-Type": "application/json"}

    def put(name, value):
        # Header names are case-insensitive: a configured header replaces ours.
        for existing in [k for k in headers if k.lower() == name.lower()]:
            del headers[existing]
        headers[name] = value

    if auth_config is None:
        return headers
    api_key = auth_config.api_key.get_secret_value() if auth_config.api_key else ""
    if api_key:
        put(auth_config.api_key_header, f"{auth_config.api_key_prefix} {api_key}".strip())
    for header in auth_config.custom_headers:
        put(header.name, header.value.get_secret_value())
    return headers


def _get_auth_config(integration: Integration) -> Optional[AuthenticateConfig]:
    auth_config = integration.get_action_config("auth")
    if not auth_config or not auth_config.data:
        return None  # an endpoint may need no credentials
    try:
        return AuthenticateConfig.parse_obj(auth_config.data)
    except pydantic.ValidationError:
        raise IntegrationConfigurationError("The Authenticate configuration is invalid.") from None


def _get_deliver_config(integration: Integration) -> DeliverConfig:
    deliver_config = integration.get_action_config("deliver")
    if not deliver_config:
        raise IntegrationConfigurationError("The Deliver action is not configured.")
    try:
        return DeliverConfig.parse_obj(deliver_config.data)
    except pydantic.ValidationError:
        raise IntegrationConfigurationError("The Deliver configuration is invalid.") from None


def _serialize(payload: pydantic.BaseModel) -> dict:
    # Through .json() so datetimes and UUIDs become JSON values jq can read.
    return json.loads(payload.json())


def _failure(error_class, message: str, *, retryable: bool, status_code: Optional[int] = None,
             retry_after: Optional[float] = None) -> IntegrationError:
    error = error_class(message, status_code)
    error.retryable = retryable
    error.retry_after = retry_after
    return error


def _is_retryable(error: Exception) -> bool:
    """Set where the failure is raised (_send): the client's verdict for HTTP outcomes."""
    return getattr(error, "retryable", False)


async def _send(integration: Integration, endpoint: EndpointSettings, input_data: Any) -> bool:
    """Shape `input_data` with the endpoint's filter and send it.

    Returns False when the filter produced nothing to send. Raises an
    IntegrationError; _is_retryable tells whether trying again could help.
    """
    should_send, body = outbound_body(endpoint.jq_filter, input_data)
    if not should_send:
        return False
    headers = build_headers(_get_auth_config(integration))
    try:
        await validate_outbound_url(
            endpoint.url, allowlist=settings.OUTBOUND_URL_ALLOWLIST,
            what=f"{OUTPUT_TYPE_TITLES[endpoint.output_type]} URL",
        )
    except URLResolutionError as e:
        # DNS hiccups pass; dropping records over one would lose data.
        raise _failure(IntegrationConnectionError, str(e), retryable=True) from None
    except ValueError as e:
        raise IntegrationConfigurationError(str(e)) from None
    try:
        await client.send_json(
            url=endpoint.url,
            method=endpoint.method.value,
            headers=headers,
            body=body,
            timeout=settings.OUTBOUND_REQUEST_TIMEOUT_SECONDS,
        )
    except client.EndpointError as e:
        raise _failure(
            _INTEGRATION_ERRORS[type(e)], e.message, retryable=e.retryable, status_code=e.status_code,
            retry_after=getattr(e, "retry_after", None),
        ) from e
    return True


def _gundi_ids(records: List[dict]) -> List[str]:
    return [str(record.get("gundi_id")) for record in records]


def _label(output_type: OutputType) -> str:
    return OUTPUT_TYPE_TITLES[output_type].lower()


async def _log_delivery_failure(
        integration: Integration, action_id: str, endpoint: EndpointSettings, records: List[dict],
        error: IntegrationError,
):
    await log_action_activity(
        integration_id=str(integration.id),
        action_id=action_id,
        title=(
            f"Could not deliver {len(records)} {_label(endpoint.output_type)}; "
            f"dropping them. {format_error_message(error)}"
        ),
        level=LogLevel.ERROR,
        data={
            "output_type": endpoint.output_type.value,
            "error_type": error.error_type,
            "status_code": error.status_code,
            "gundi_ids": _gundi_ids(records),
        },
    )


async def _log_missing_url(integration: Integration, action_id: str, output_type: OutputType, records: List[dict]):
    await log_action_activity(
        integration_id=str(integration.id),
        action_id=action_id,
        title=(
            f"Dropping {len(records)} {_label(output_type)}: the type is selected for delivery "
            f"but has no URL. Set '{OUTPUT_TYPE_TITLES[output_type]}: URL' in the Deliver configuration."
        ),
        level=LogLevel.ERROR,
        data={"output_type": output_type.value, "gundi_ids": _gundi_ids(records)},
    )


async def _log_dropped(integration: Integration, action_id: str, payloads: List[pydantic.BaseModel]):
    counts = Counter(type(payload).__name__ for payload in payloads)
    summary = ", ".join(f"{count} {name}" for name, count in sorted(counts.items()))
    await log_action_activity(
        integration_id=str(integration.id),
        action_id=action_id,
        title=f"Dropping {summary}: not a data type selected for delivery.",
        level=LogLevel.INFO,
        data={"payload_types": dict(counts), "gundi_ids": [str(p.gundi_id) for p in payloads]},
    )


async def _flush_buffer(
        integration: Integration, action_id: str, endpoint: EndpointSettings, *, drain: bool = False,
) -> dict:
    """Send what is due in the endpoint's buffer; with drain, everything in it.

    A permanent failure drops the batch (logged) so it cannot block the buffer.
    A transient one keeps it and backs the buffer off, skipping flushes until
    the delay passes; the WARNING is published once per failed attempt, so at
    most once per backoff window. Never raises.
    """
    integration_id, output_type = str(integration.id), endpoint.output_type.value
    if remaining := await outbound_buffer.backoff_remaining(integration_id, output_type):
        return {"backing_off_seconds": round(remaining)}

    async def send(records):
        try:
            await _send(integration, endpoint, records if endpoint.batch_mode else records[0])
        except IntegrationError as e:
            if _is_retryable(e):
                raise
            await _log_delivery_failure(integration, action_id, endpoint, records, e)

    try:
        result = await outbound_buffer.flush(
            integration_id, output_type,
            # A buffer left behind by single mode is drained one record per read
            # and trim, so a failure mid-drain never re-sends delivered records.
            max_batch_size=endpoint.max_batch_size if endpoint.batch_mode else 1,
            max_wait_seconds=0 if drain else endpoint.max_wait_seconds,
            send=send,
        )
    except Exception as e:
        error = format_error_message(e) or f"{type(e).__name__}: {e}"
        logger.warning(f"Flushing the {output_type} buffer of integration '{integration_id}' failed: {error}")
        try:
            delay = round(await outbound_buffer.start_backoff(
                integration_id, output_type, retry_after=getattr(e, "retry_after", None),
            ))
        except Exception as backoff_error:
            logger.warning(f"Could not store the flush backoff: {type(backoff_error).__name__}: {backoff_error}")
            delay = None
        await log_action_activity(
            integration_id=integration_id,
            action_id=action_id,
            title=f"Could not send buffered {_label(endpoint.output_type)}; will retry"
                  f"{f' in {delay} s' if delay else ''}. {error}",
            level=LogLevel.WARNING,
            data={"output_type": output_type, "retry_in_seconds": delay},
        )
        return {"error": error, "retry_in_seconds": delay}
    if result.batches_sent:
        try:
            await outbound_buffer.clear_backoff(integration_id, output_type)
        except Exception as e:
            logger.warning(f"Could not reset the flush backoff: {type(e).__name__}: {e}")
    return result._asdict()


async def _drop_buffer(integration: Integration, action_id: str, output_type: OutputType) -> dict:
    """Empty a buffer whose type no longer has a URL, logging what is dropped."""
    async def drop(records):
        await _log_missing_url(integration, action_id, output_type, records)

    result = await outbound_buffer.flush(
        str(integration.id), output_type.value,
        max_batch_size=settings.OUTBOUND_BUFFER_MAX_RECORDS, max_wait_seconds=0, send=drop,
    )
    return {**result._asdict(), "dropped": True}


async def _report_buffer_overflow(integration: Integration, output_type: OutputType, dropped: int):
    """ERROR for records the buffer cap dropped, at most once per
    BUFFER_OVERFLOW_REPORT_SECONDS, with the count since the last report."""
    integration_id = str(integration.id)
    await outbound_buffer.add_dropped(integration_id, output_type.value, dropped)
    try:
        first_in_window = await state_manager.set_if_absent(
            integration_id=integration_id, action_id="deliver",
            source_id=f"buffer-overflow-{output_type.value}", ttl_seconds=BUFFER_OVERFLOW_REPORT_SECONDS,
        )
    except Exception:
        first_in_window = True  # surface it rather than hide it when the throttle is unavailable
    if not first_in_window:
        return
    total = await outbound_buffer.take_dropped(integration_id, output_type.value)
    if total:
        await log_action_activity(
            integration_id=integration_id,
            action_id="deliver",
            title=(
                f"The {_label(output_type)} buffer is full; dropped {total} of its oldest record(s) "
                f"since the last report. The endpoint has not accepted data for a while."
            ),
            level=LogLevel.ERROR,
            data={"output_type": output_type.value, "dropped": total},
        )


async def action_auth(integration: Integration, action_config: AuthenticateConfig):
    # The runner has already validated the config. There is nothing to probe:
    # endpoints are arbitrary, and a test request would deliver data.
    return {"valid_credentials": True}


# Errors only: cdip-routing sends one GundiDelivery per record, so started and
# completed events would add two activity entries per record. They could also
# fail after a send or a buffer push and get the record redelivered twice.
@activity_logger(on_start=False, on_completion=False)
async def action_deliver(
        integration: Integration,
        action_config: DeliverConfig,
        data: GundiDelivery,
        metadata: dict,
):
    payload = apply_transformations(
        data.payload,
        data.route_configuration,
        provider_id=data.provider.provider_id,
        destination_id=str(integration.id),
    )
    output_type = _OUTPUT_TYPES_BY_PAYLOAD.get(type(payload))
    if output_type not in action_config.output_types:
        await _log_dropped(integration, "deliver", [payload])
        return {"dropped": True, "payload_type": type(payload).__name__}

    endpoint = action_config.endpoint_for(output_type)
    record = _serialize(payload)
    if not endpoint.url:
        await _log_missing_url(integration, "deliver", output_type, [record])
        return {"dropped": True, "output_type": output_type.value, "reason": "missing_url"}

    if endpoint.batch_mode:
        # Once buffered the record is ours to deliver: a failed flush below must
        # not fail the run, or Pub/Sub would redeliver and buffer it twice.
        pushed = await outbound_buffer.push(str(integration.id), output_type.value, record)
        if pushed.dropped:
            await _report_buffer_overflow(integration, output_type, pushed.dropped)
        flushed = await _flush_buffer(integration, "deliver", endpoint)
        return {"buffered": True, "output_type": output_type.value, "buffer_length": pushed.length, "flush": flushed}

    try:
        sent = await _send(integration, endpoint, record)
    except IntegrationError as e:
        if _is_retryable(e):
            raise
        await _log_delivery_failure(integration, "deliver", endpoint, [record], e)
        return {"delivered": False, "output_type": output_type.value, "error": format_error_message(e)}
    return {"delivered": sent, "output_type": output_type.value}


async def _deliver_bundle_group(
        integration: Integration, batch_id, endpoint: EndpointSettings, records: List[dict],
) -> dict:
    if not endpoint.url:
        await _log_missing_url(integration, "deliver_batch", endpoint.output_type, records)
        return {"dropped": len(records), "reason": "missing_url"}
    chunk_size = endpoint.max_batch_size if endpoint.batch_mode else 1
    chunks = list(generate_batches(records, chunk_size))
    progress_args = (batch_id, str(integration.id), endpoint.output_type.value)
    fp = fingerprint(_gundi_ids(records), chunk_size)
    # A bit is set once its request is settled: delivered, or permanently
    # rejected and logged. Either way a redelivered bundle must not send it again.
    settled = decode(await batch_progress.read(*progress_args), fp, len(chunks))
    result = {"requests": len(chunks), "already_settled": len(settled), "sent": 0, "skipped_by_filter": 0, "failed": 0}
    for index, chunk in enumerate(chunks):
        if index in settled:
            continue
        try:
            sent = await _send(integration, endpoint, chunk if endpoint.batch_mode else chunk[0])
        except IntegrationError as e:
            if _is_retryable(e):
                # Earlier requests are already recorded, so the redelivery resumes here.
                raise
            await _log_delivery_failure(integration, "deliver_batch", endpoint, chunk, e)
            result["failed"] += 1
        else:
            result["sent" if sent else "skipped_by_filter"] += 1
        settled.add(index)
        await batch_progress.write(*progress_args, fp, settled, len(chunks))
    return result


@activity_logger(on_start=False, on_completion=False)
async def action_deliver_batch(
        integration: Integration,
        action_config: DeliverBatchConfig,
        data: GundiBatchDelivery,
        metadata: dict,
):
    """Deliver a GundiBatchDelivery bundle with the `deliver` action's config.

    One config form serves both actions, so `action_config` is only the
    marker that routes the bundle here (see DeliverBatchConfig). Batch-mode
    types are sent in chunks of max_batch_size straight from the bundle,
    without the buffer; single-mode types send one request per record.
    """
    try:
        deliver_config = _get_deliver_config(integration)
    except IntegrationConfigurationError as e:
        # Acked: redelivering cannot help until someone fixes the config, and
        # /push-data would otherwise redeliver the bundle until it expires.
        await log_action_activity(
            integration_id=str(integration.id),
            action_id="deliver_batch",
            title=f"Dropping a bundle of {len(data.payloads)} record(s): {e.message}",
            level=LogLevel.ERROR,
            data={"batch_id": str(data.batch_id), "gundi_ids": [str(p.gundi_id) for p in data.payloads]},
        )
        return {"batch_id": str(data.batch_id), "dropped": len(data.payloads), "reason": "deliver_not_configured"}

    groups: Dict[OutputType, List[dict]] = {}
    dropped = []
    for payload in data.payloads:
        payload = apply_transformations(
            payload,
            data.route_configuration,
            provider_id=data.provider.provider_id,
            destination_id=str(integration.id),
        )
        output_type = _OUTPUT_TYPES_BY_PAYLOAD.get(type(payload))
        if output_type not in deliver_config.output_types:
            dropped.append(payload)
            continue
        groups.setdefault(output_type, []).append(_serialize(payload))
    if dropped:
        await _log_dropped(integration, "deliver_batch", dropped)

    delivered = {}
    for output_type, records in groups.items():
        delivered[output_type.value] = await _deliver_bundle_group(
            integration, data.batch_id, deliver_config.endpoint_for(output_type), records,
        )
    return {"batch_id": str(data.batch_id), "dropped": len(dropped), "delivered": delivered}


async def _warn_invalid_deliver_config(integration: Integration, error: IntegrationConfigurationError):
    logger.warning(f"flush_buffers skipped for integration '{integration.id}': {error.message}")
    try:
        first_in_window = await state_manager.set_if_absent(
            integration_id=str(integration.id), action_id="flush_buffers",
            source_id="invalid-deliver-config-warning", ttl_seconds=INVALID_CONFIG_WARNING_THROTTLE_SECONDS,
        )
    except Exception:
        first_in_window = True  # surface it rather than hide it when the throttle is unavailable
    if first_in_window:
        await log_action_activity(
            integration_id=str(integration.id),
            action_id="flush_buffers",
            title=f"Not flushing buffered records: {error.message} Buffered records wait until it is fixed.",
            level=LogLevel.WARNING,
        )


# No @activity_logger: this fires every minute for every integration of the
# type, and started/completed events for each run would bury the activity feed.
@crontab_schedule("* * * * *")
async def action_flush_buffers(integration: Integration, action_config: FlushBuffersConfig):
    """Send batch-mode buffers whose oldest record has waited max_wait_seconds,
    and drain buffers left behind when a type leaves batch mode or delivery."""
    if not integration.get_action_config("deliver"):
        return {"skipped": True, "reason": "deliver_not_configured"}
    try:
        deliver_config = _get_deliver_config(integration)
    except IntegrationConfigurationError as e:
        await _warn_invalid_deliver_config(integration, e)
        return {"skipped": True, "reason": "invalid_deliver_configuration"}
    flushed = {}
    for output_type in OutputType:
        endpoint = deliver_config.endpoint_for(output_type)
        active = output_type in deliver_config.output_types and endpoint.batch_mode
        if active and endpoint.url:
            flushed[output_type.value] = await _flush_buffer(integration, "flush_buffers", endpoint)
        elif await outbound_buffer.length(str(integration.id), output_type.value):
            if endpoint.url:
                flushed[output_type.value] = await _flush_buffer(integration, "flush_buffers", endpoint, drain=True)
            else:
                flushed[output_type.value] = await _drop_buffer(integration, "flush_buffers", output_type)
    return {"flushed": flushed}
