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

if OUTBOUND_FLUSH_LOCK_SECONDS <= 2 * OUTBOUND_REQUEST_TIMEOUT_SECONDS:
    # The flush loop stops starting sends 2 timeouts before its lock expires,
    # so with this setting it would never send and buffers would never drain.
    raise ValueError(
        f"OUTBOUND_FLUSH_LOCK_SECONDS ({OUTBOUND_FLUSH_LOCK_SECONDS}) must be greater than "
        f"2 x OUTBOUND_REQUEST_TIMEOUT_SECONDS ({OUTBOUND_REQUEST_TIMEOUT_SECONDS})."
    )
