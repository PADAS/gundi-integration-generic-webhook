# Add your integration-specific settings here
from .base import env

# Outbound webhooks (app/actions/handlers.py)
OUTBOUND_REQUEST_TIMEOUT_SECONDS = env.float("OUTBOUND_REQUEST_TIMEOUT_SECONDS", 30.0)
# Empty means any public https host; set it to restrict where integrations may push data.
OUTBOUND_URL_ALLOWLIST = env.list("OUTBOUND_URL_ALLOWLIST", [])
# Upper bound on one buffer flush. A flusher that outlives its lock could have its
# buffer flushed concurrently, so the flush loop stops sending well before it expires.
OUTBOUND_FLUSH_LOCK_SECONDS = env.int("OUTBOUND_FLUSH_LOCK_SECONDS", 300)
# A bundle redelivered after its record expires is re-sent in full (duplicates, never loss).
OUTBOUND_BATCH_PROGRESS_TTL_SECONDS = env.int("OUTBOUND_BATCH_PROGRESS_TTL_SECONDS", 25 * 60 * 60)
# Past this many records the oldest are dropped (and logged): an endpoint down for
# days must not grow Redis without bound.
OUTBOUND_BUFFER_MAX_RECORDS = env.int("OUTBOUND_BUFFER_MAX_RECORDS", 10000)
# After a transient flush failure the buffer waits before the next attempt,
# doubling per consecutive failure up to the max. A 429's Retry-After wins.
OUTBOUND_BACKOFF_INITIAL_SECONDS = env.int("OUTBOUND_BACKOFF_INITIAL_SECONDS", 30)
OUTBOUND_BACKOFF_MAX_SECONDS = env.int("OUTBOUND_BACKOFF_MAX_SECONDS", 900)

# A full buffer's drops are reported to the activity log at most this often.
OUTBOUND_OVERFLOW_REPORT_SECONDS = env.int("OUTBOUND_OVERFLOW_REPORT_SECONDS", 600)

_OUTBOUND_POSITIVE_INTS = (
    "OUTBOUND_FLUSH_LOCK_SECONDS",
    "OUTBOUND_BATCH_PROGRESS_TTL_SECONDS",
    "OUTBOUND_BUFFER_MAX_RECORDS",
    "OUTBOUND_BACKOFF_INITIAL_SECONDS",
    "OUTBOUND_BACKOFF_MAX_SECONDS",
    "OUTBOUND_OVERFLOW_REPORT_SECONDS",
)


def validate_outbound_settings(values: dict) -> None:
    """Raise ValueError naming the first outbound setting that cannot work.

    These reach Redis as SETEX/EXPIRE/SET EX times and LTRIM bounds, where zero
    or a negative value is an error at runtime or silently empties a buffer.
    """
    for name in _OUTBOUND_POSITIVE_INTS:
        value = values[name]
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{name} must be a positive integer; got {value!r}.")
    timeout = values["OUTBOUND_REQUEST_TIMEOUT_SECONDS"]
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not timeout > 0:
        raise ValueError(f"OUTBOUND_REQUEST_TIMEOUT_SECONDS must be a positive number; got {timeout!r}.")
    if values["OUTBOUND_BACKOFF_INITIAL_SECONDS"] > values["OUTBOUND_BACKOFF_MAX_SECONDS"]:
        raise ValueError(
            f"OUTBOUND_BACKOFF_INITIAL_SECONDS ({values['OUTBOUND_BACKOFF_INITIAL_SECONDS']}) must not exceed "
            f"OUTBOUND_BACKOFF_MAX_SECONDS ({values['OUTBOUND_BACKOFF_MAX_SECONDS']})."
        )
    if values["OUTBOUND_FLUSH_LOCK_SECONDS"] <= 2 * timeout:
        # The flush loop stops starting sends 2 timeouts before its lock expires,
        # so with this setting it would never send and buffers would never drain.
        raise ValueError(
            f"OUTBOUND_FLUSH_LOCK_SECONDS ({values['OUTBOUND_FLUSH_LOCK_SECONDS']}) must be greater than "
            f"2 x OUTBOUND_REQUEST_TIMEOUT_SECONDS ({timeout})."
        )


validate_outbound_settings(globals())
