import asyncio
import logging
from time import monotonic
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple

import pydantic

# app.settings before gundi_client_v2 (see app/services/errors.py).
from app import settings
from gundi_client_v2.transformations import apply_transformations
from gundi_core import schemas
from gundi_core.events import GundiDelivery
from gundi_core.schemas.v2 import Integration, LogLevel

from app.services.activity_logger import activity_logger, log_action_activity
from app.services.batch_progress import BatchProgressStore, decode, fingerprint, headers_digest
from app.services.errors import (
    IntegrationAuthError,
    IntegrationBadResponseError,
    IntegrationConfigurationError,
    IntegrationConnectionError,
    IntegrationError,
    IntegrationRateLimitError,
    format_error_message,
)
from app.services.jq_transform import jq_input, outbound_body
from app.services.outbound_buffer import OutboundBuffer
from app.services.outbound_samples import OutboundSamples
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
    GetDataSamplesQuery,
    OutputType,
)
from .envelopes import GundiBatchDelivery

logger = logging.getLogger(__name__)

outbound_buffer = OutboundBuffer()
batch_progress = BatchProgressStore()
outbound_samples = OutboundSamples()
state_manager = IntegrationStateManager()

# (integration_id, output_type) -> monotonic time of the last samples DEL. Capture
# is off by default, so without it every record of every integration would DEL.
_samples_cleared_at: Dict[Tuple[str, str], float] = {}
_SAMPLES_CLEAR_INTERVAL_SECONDS = 60
_SAMPLES_CLEARED_MAX_ENTRIES = 10000


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


async def _log_stranded(integration: Integration, action_id: str, output_type: OutputType, records: List[dict]):
    await log_action_activity(
        integration_id=str(integration.id),
        action_id=action_id,
        title=(
            f"Dropping {len(records)} buffered {_label(output_type)}: the Deliver configuration "
            f"no longer has an endpoint for {OUTPUT_TYPE_TITLES[output_type]}."
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
        title=f"Dropping {summary}: the Deliver configuration has no endpoint for this data type.",
        level=LogLevel.INFO,
        data={"payload_types": dict(counts), "gundi_ids": [str(p.gundi_id) for p in payloads]},
    )


async def _capture_samples(integration: Integration, endpoint: EndpointSettings, records: List[dict]):
    """Keep the newest records as samples for the transformation editor, or
    delete the type's samples when its endpoint has capture off (at most once
    per _SAMPLES_CLEAR_INTERVAL_SECONDS per process).

    Never raises, and gives Redis OUTBOUND_SAMPLES_TIMEOUT_SECONDS: samples are
    a convenience, and an exception or a stall here would fail the delivery
    and have Pub/Sub redeliver the records.
    """
    integration_id, output_type = str(integration.id), endpoint.output_type.value
    cleared_key = (integration_id, output_type)
    try:
        if endpoint.capture_samples:
            # So turning capture off again deletes at the next record.
            _samples_cleared_at.pop(cleared_key, None)
            await asyncio.wait_for(
                outbound_samples.capture(integration_id, output_type, records),
                timeout=settings.OUTBOUND_SAMPLES_TIMEOUT_SECONDS,
            )
            return
        now = monotonic()
        last = _samples_cleared_at.get(cleared_key)
        if last is not None and now - last < _SAMPLES_CLEAR_INTERVAL_SECONDS:
            return
        await asyncio.wait_for(
            outbound_samples.clear(integration_id, output_type), timeout=settings.OUTBOUND_SAMPLES_TIMEOUT_SECONDS,
        )
        if len(_samples_cleared_at) >= _SAMPLES_CLEARED_MAX_ENTRIES:
            _samples_cleared_at.clear()
        _samples_cleared_at[cleared_key] = now
    except Exception as e:
        logger.warning(
            f"Could not update the {output_type} samples of integration '{integration_id}': {type(e).__name__}: {e}"
        )


async def _flush_buffer(
        integration: Integration, action_id: str, endpoint: EndpointSettings, *, drain: bool = False,
) -> dict:
    """_flush_due, then report what the buffer cap dropped: on push, or when
    this flush released its lock.

    Never raises: callers run it after a record is pushed, and once pushed the
    buffer owns the record. An exception escaping here would fail the delivery,
    and the Pub/Sub redelivery would buffer the record a second time.
    """
    try:
        return await _flush_due(integration, action_id, endpoint, drain=drain)
    except Exception as e:
        error = f"{type(e).__name__}: {e}"
        logger.warning(
            f"Flushing the {endpoint.output_type.value} buffer of integration '{integration.id}' failed: {error}"
        )
        return {"error": error}
    finally:
        await _report_buffer_overflow(integration, action_id, endpoint.output_type)


async def _flush_due(
        integration: Integration, action_id: str, endpoint: EndpointSettings, *, drain: bool = False,
) -> dict:
    """Send what is due in the endpoint's buffer; with drain, everything in it.

    A permanent failure drops the batch (logged) so it cannot block the buffer.
    A transient one keeps it and backs the buffer off, skipping flushes until
    the delay passes; the WARNING is published once per failed attempt, so at
    most once per backoff window. Call it through _flush_buffer, which keeps
    its Redis reads and activity publishing from raising.
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
    """Empty a buffer whose type no longer has an endpoint, logging what is dropped."""
    async def drop(records):
        await _log_stranded(integration, action_id, output_type, records)

    result = await outbound_buffer.flush(
        str(integration.id), output_type.value,
        max_batch_size=settings.OUTBOUND_BUFFER_MAX_RECORDS, max_wait_seconds=0, send=drop,
    )
    return {**result._asdict(), "dropped": True}


async def _report_buffer_overflow(integration: Integration, action_id: str, output_type: OutputType):
    """ERROR for records the buffer cap dropped, at most once per
    OUTBOUND_OVERFLOW_REPORT_SECONDS, with the count since the last report.

    The count is acknowledged only after the ERROR is published, and only the
    count it names, so drops are never lost to a failed publish and drops that
    land meanwhile go into the next report. Without the throttle window there
    is no report. Never raises.
    """
    integration_id = str(integration.id)
    window = dict(
        integration_id=integration_id, action_id="deliver", source_id=f"buffer-overflow-{output_type.value}",
    )
    try:
        count = await outbound_buffer.pending_dropped(integration_id, output_type.value)
        if count <= 0:
            return
        # The window is also what keeps concurrent deliveries from reporting
        # the same count. If it cannot be taken, the count waits for a later
        # delivery, so this raises into the except below rather than report.
        if not await state_manager.set_if_absent(**window, ttl_seconds=settings.OUTBOUND_OVERFLOW_REPORT_SECONDS):
            return
        try:
            await log_action_activity(
                integration_id=integration_id,
                action_id=action_id,
                title=(
                    f"The {_label(output_type)} buffer is full; dropped {count} of its oldest record(s) "
                    f"since the last report. The endpoint has not accepted data for a while."
                ),
                level=LogLevel.ERROR,
                data={"output_type": output_type.value, "dropped": count},
            )
        except Exception:
            # Unreported: reopen the window so the next flush tries again.
            await state_manager.delete_state(**window)
            raise
        await outbound_buffer.acknowledge_dropped(integration_id, output_type.value, count)
    except Exception as e:
        logger.warning(f"Could not report buffer overflow for integration '{integration_id}': {type(e).__name__}: {e}")


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
    result, flushed_type = await _deliver_payload(integration, action_config, data)
    if swept := await _sweep_buffers(integration, "deliver", action_config, exclude=flushed_type):
        result["other_buffers"] = swept
    return result


async def _deliver_payload(
        integration: Integration, action_config: DeliverConfig, data: GundiDelivery,
) -> Tuple[dict, Optional[OutputType]]:
    """Deliver or buffer one payload. Returns the result and the type whose
    buffer was just flushed, if any, so the sweep can skip it."""
    payload = apply_transformations(
        data.payload,
        data.route_configuration,
        provider_id=data.provider.provider_id,
        destination_id=str(integration.id),
    )
    output_type = _OUTPUT_TYPES_BY_PAYLOAD.get(type(payload))
    endpoint = action_config.endpoint_for(output_type) if output_type else None
    if endpoint is None:
        await _log_dropped(integration, "deliver", [payload])
        return {"dropped": True, "payload_type": type(payload).__name__}, None

    record = jq_input(payload)
    await _capture_samples(integration, endpoint, [record])

    if endpoint.batch_mode:
        # Once buffered the record is ours to deliver: a failed flush below must
        # not fail the run, or Pub/Sub would redeliver and buffer it twice.
        pushed = await outbound_buffer.push(str(integration.id), output_type.value, record)
        flushed = await _flush_buffer(integration, "deliver", endpoint)
        return (
            {"buffered": True, "output_type": output_type.value, "buffer_length": pushed.length, "flush": flushed},
            output_type,
        )

    try:
        sent = await _send(integration, endpoint, record)
    except IntegrationError as e:
        if _is_retryable(e):
            raise
        await _log_delivery_failure(integration, "deliver", endpoint, [record], e)
        return {"delivered": False, "output_type": output_type.value, "error": format_error_message(e)}, None
    return {"delivered": sent, "output_type": output_type.value}, None


def _request_plan(integration: Integration, endpoint: EndpointSettings) -> list:
    """What a bundle's requests depend on besides its records, for the progress
    fingerprint: any change to it re-sends requests recorded as settled."""
    try:
        auth = headers_digest(build_headers(_get_auth_config(integration)))
    except IntegrationConfigurationError:
        auth = b"invalid"  # every send fails the same way until it is fixed
    return [
        endpoint.url, endpoint.method.value, endpoint.jq_filter,
        "batch" if endpoint.batch_mode else "single", auth,
    ]


async def _deliver_bundle_group(
        integration: Integration, batch_id, endpoint: EndpointSettings, records: List[dict],
) -> dict:
    chunk_size = endpoint.max_batch_size if endpoint.batch_mode else 1
    chunks = list(generate_batches(records, chunk_size))
    progress_args = (batch_id, str(integration.id), endpoint.output_type.value)
    fp = fingerprint(_gundi_ids(records), chunk_size, _request_plan(integration, endpoint))
    # A bit is set once its request is settled: delivered, or permanently
    # rejected and logged. Either way a redelivered bundle with the same request
    # plan must not send it again.
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
        groups.setdefault(output_type, []).append(jq_input(payload))
    if dropped:
        await _log_dropped(integration, "deliver_batch", dropped)

    delivered = {}
    for output_type, records in groups.items():
        endpoint = deliver_config.endpoint_for(output_type)
        await _capture_samples(integration, endpoint, records)
        delivered[output_type.value] = await _deliver_bundle_group(integration, data.batch_id, endpoint, records)
    result = {"batch_id": str(data.batch_id), "dropped": len(dropped), "delivered": delivered}
    if swept := await _sweep_buffers(integration, "deliver_batch", deliver_config):
        result["buffers"] = swept
    return result


# No @activity_logger: the portal calls this each time the transformation
# editor opens or refreshes, which would flood the integration's activity feed.
async def action_get_data_samples(integration: Integration, action_config: GetDataSamplesQuery):
    """Captured samples (newest first) and which types are capturing them.

    Reference actions get no stored config, so whether capture is on is read
    from the integration's current deliver config. Types with capture off
    return no samples even if some are still stored: deliveries delete them
    only when a record of the type arrives, so this also deletes them.
    """
    integration_id = str(integration.id)
    try:
        deliver_config = _get_deliver_config(integration)
    except IntegrationConfigurationError:
        deliver_config = None
    capture_enabled = {}
    for output_type in OutputType:
        endpoint = deliver_config.endpoint_for(output_type) if deliver_config else None
        capture_enabled[output_type.value] = bool(endpoint and endpoint.capture_samples)
    if disabled := [name for name, enabled in capture_enabled.items() if not enabled]:
        try:
            await asyncio.wait_for(
                outbound_samples.clear(integration_id, *disabled), timeout=settings.OUTBOUND_SAMPLES_TIMEOUT_SECONDS,
            )
        except Exception as e:
            logger.warning(
                f"Could not delete the disabled samples of integration '{integration_id}': {type(e).__name__}: {e}"
            )
    output_types = [action_config.output_type] if action_config.output_type else list(OutputType)
    samples = {
        output_type.value: (
            await outbound_samples.read(integration_id, output_type.value)
            if capture_enabled[output_type.value] else []
        )
        for output_type in output_types
    }
    return {
        "samples": samples,
        "capture_enabled": capture_enabled,
        "max_samples": settings.OUTBOUND_SAMPLES_MAX,
        "ttl_seconds": settings.OUTBOUND_SAMPLES_TTL_SECONDS,
    }


async def _sweep_buffers(
        integration: Integration, action_id: str, deliver_config: DeliverConfig,
        exclude: Optional[OutputType] = None,
) -> dict:
    """Move the integration's buffers along on any delivery, since nothing
    flushes them on a schedule: flush a batch-mode buffer that is due, drain one
    whose endpoint left batch mode, drop (logged) one whose type lost its
    endpoint. Empty and not-yet-due buffers cost one or two reads each. Never
    raises: a failure here must not fail the delivery that triggered it."""
    integration_id, swept = str(integration.id), {}
    for output_type in OutputType:
        if output_type == exclude:
            continue
        try:
            endpoint = deliver_config.endpoint_for(output_type)
            if endpoint and endpoint.batch_mode:
                if await outbound_buffer.is_due(
                        integration_id, output_type.value, endpoint.max_batch_size, endpoint.max_wait_seconds,
                ):
                    swept[output_type.value] = await _flush_buffer(integration, action_id, endpoint)
            elif await outbound_buffer.length(integration_id, output_type.value):
                if endpoint:
                    swept[output_type.value] = await _flush_buffer(integration, action_id, endpoint, drain=True)
                else:
                    swept[output_type.value] = await _drop_buffer(integration, action_id, output_type)
        except Exception as e:
            logger.warning(
                f"Could not sweep the {output_type.value} buffer of integration '{integration_id}': "
                f"{type(e).__name__}: {e}"
            )
            swept[output_type.value] = {"error": f"{type(e).__name__}: {e}"}
    return swept
