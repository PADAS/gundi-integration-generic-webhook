# Outbound webhooks (generic push actions)

Status: proof of concept (`hackathon-outbound-webhooks`).

This connector can also be a **destination**: data that Gundi routes to the
integration is shaped with a JQ filter and sent as JSON to HTTPS endpoints
the user configures. It needs the destination integration to have
`additional.generic_model = true`, so cdip-routing publishes generic
`GundiDelivery` envelopes instead of destination-specific payloads.

## Actions

| Action | Type | Trigger | Purpose |
|---|---|---|---|
| `auth` | auth | portal | API key and custom headers sent on every request |
| `deliver` | push | `GundiDelivery` | Delivers one payload (or buffers it in batch mode) |
| `deliver_batch` | push, internal | `GundiBatchDelivery` | Delivers a bundle. Uses the `deliver` config; not registered, so no form of its own |
| `flush_buffers` | pull | every minute | Sends due batch-mode buffers; drains buffers left behind by config changes |

Code: `app/actions/handlers.py`, models in `app/actions/configurations.py`,
HTTP client in `app/actions/client.py`, bundle envelope in
`app/actions/envelopes.py`, and in `app/services/`: `jq_transform.py`,
`outbound_buffer.py` and `batch_progress.py`.

`deliver` and `deliver_batch` publish activity events on **errors only**.
cdip-routing sends one `GundiDelivery` per record, so started/completed
events would add two feed entries per record.

## Configuration

**Authenticate**

- `api_key` (secret, optional): sent as `{api_key_header}: {api_key_prefix} {api_key}`.
  An empty prefix sends the raw key.
- `api_key_header` (default `Authorization`), `api_key_prefix` (default `Bearer`).
- `custom_headers`: list of `{name, value}`. Values are stored as secrets.
  A custom header with the same name as one of ours (case-insensitive) replaces it,
  `Content-Type` included.
- Header names must be valid HTTP tokens. Values, the key and the prefix are
  stripped and must be ASCII with no line breaks or other control
  characters. These are rejected when saved, because they could never be
  sent.

**Deliver**

- `output_types`: one or more of Observations, Events, Event Updates,
  Messages (stored as `observation`, `event`, `event_update`, `message`).
  Anything else routed here (attachments, unselected types) is dropped with
  an INFO entry in the activity log.
- For each type `<t>` (form fields are flat because the portal renders them more reliably):
  - `<t>_url` (secret, password widget): webhook URLs are often credentials
    (Slack-style hook paths, `?token=`), so they never appear in activity-log
    config data.
    - Must be `https` and resolve to public addresses only
      (`app/services/url_policy.py`). It is checked before every request, and
      redirects are not followed. Set `OUTBOUND_URL_ALLOWLIST` to restrict hosts
      further.
    - A selected type with **no URL is not a validation error**: the portal form
      cannot enforce the rule, and a config that fails to parse would fail every
      message routed here. Instead, records of that type are dropped with an
      ERROR naming the missing field.
  - `<t>_method`: `POST` (default) or `PUT`.
  - `<t>_jq_filter`: default `.`.
  - `<t>_batch_mode` (default off), `<t>_max_batch_size` (default 100, ≥ 1),
    `<t>_max_wait_seconds` (default 60, ≥ 60, since the flush runs once a minute).

**Flush Buffers**: only the standard `run_on_schedule` toggle, and nothing to
fill in. It runs for every integration of the type, **whether or not this form
was saved**. Its config model mixes in `StoredConfigOptionalMixin`
(`app/actions/core.py`). The runner then executes the action with the model's
defaults when no row is stored, instead of skipping it like other scheduled
pulls; this is a one-line, opt-in hook in `action_runner.py`. A stored row
still applies, so `run_on_schedule = false` pauses it. Integrations without a
`deliver` config return at once.

Settings (`app/settings/integration.py`):

| Setting | Default |
|---|---|
| `OUTBOUND_REQUEST_TIMEOUT_SECONDS` | 30 |
| `OUTBOUND_URL_ALLOWLIST` | empty: any public host |
| `OUTBOUND_FLUSH_LOCK_SECONDS` | 300; must exceed 2 × the request timeout, checked at startup |
| `OUTBOUND_BATCH_PROGRESS_TTL_SECONDS` | 25 h |
| `OUTBOUND_BUFFER_MAX_RECORDS` | 10000 |
| `OUTBOUND_BACKOFF_INITIAL_SECONDS` | 30 |
| `OUTBOUND_BACKOFF_MAX_SECONDS` | 900 |

## JQ semantics

Payloads are serialized with pydantic `.json()` first, so datetimes and UUIDs
reach the filter as strings. The filter's input is:

- single mode: one record (an object);
- batch mode: an array of records.

Its outputs decide the request (`null` outputs are discarded first):

- **no output**: no request. Use `select(...)` or yield `null` to filter records out.
- **one output**: that value is the body.
- **several outputs**: sent as one JSON array (e.g. `.[] | {id: .gundi_id}` in batch mode).

A filter that fails to compile or to run is a configuration error. Delivery
treats it like any other permanent failure (below). **In batch mode the
filter runs on the whole chunk, so a runtime error caused by one malformed
record drops the whole chunk** (logged with every `gundi_id`). Write batch
filters defensively, e.g. `map(.x | tonumber? // null)`. Line breaks are
removed before the filter runs, as the inbound webhook always did.

## Delivery and failure handling

The endpoint's answer is classified by the client (`EndpointError.retryable`
is the single source of truth for HTTP outcomes):

| Answer | Classified as | Handling |
|---|---|---|
| 2xx | delivered | |
| 429 | rate limit | retryable (Retry-After honoured in batch mode) |
| 5xx, 408, 425, connection error, timeout | bad response / connectivity | retryable |
| DNS failure while checking the URL | connectivity | retryable |
| 401, 403 | auth | permanent |
| other 4xx, 3xx (the message names the redirect target host) | bad response | permanent |
| URL refused by policy, bad filter, invalid auth config, header the client cannot send | configuration | permanent |
| selected type with no URL | | dropped, ERROR |

**Retryable** failures reach the runner as an exception. `/push-data` then
answers non-2xx, and Pub/Sub redelivers with backoff.
**Permanent** failures are logged to the activity log (ERROR, with the
affected `gundi_id`s) and the message is acked, so a record the endpoint
will never accept does not get redelivered until it expires. The cost is that
records rejected for a fixable reason (e.g. revoked credentials) are dropped,
not held.

A bundle that arrives while the `deliver` config is missing or invalid is
also logged (ERROR) and acked.

### Single mode (`deliver`)

One request per payload, at least once: a retryable failure is redelivered,
which can produce a duplicate.

### Batch mode (`deliver` + buffer)

Each record is appended to a Redis list per (integration, output type),
stamped with its enqueue time. A flush is due when the buffer holds
`max_batch_size` records or its oldest record is older than
`max_wait_seconds`. It runs:

- inline, after every `deliver` into that buffer;
- every minute, from `flush_buffers`.

A flush holds a per-buffer lock (`SET NX EX` with a token, released by a
token-checked script). It reads up to `max_batch_size` records, sends them,
and trims them only after a 2xx. The trim also checks the token, so a flusher
whose lock expired mid-send never drops records it did not send.

Guarantees: at least once (a crash between the 2xx and the trim re-sends the
batch). Ordering holds per buffer, except when a permanently rejected batch
is dropped. Once a record is buffered, a failed inline flush does not fail
the `deliver` run, since a redelivery would buffer the record twice.

- **Backoff**: a transient failure leaves the batch at the head and backs the
  buffer off. The delay is the 429's Retry-After (capped at 24 h), or else
  `OUTBOUND_BACKOFF_INITIAL_SECONDS` doubled per consecutive failure, up to
  `OUTBOUND_BACKOFF_MAX_SECONDS`. Inline and scheduled flushes skip the buffer
  until the delay passes. The WARNING ("will retry in N s") is published once
  per failed attempt, so at most once per backoff window. A successful batch
  resets the streak.
- **Cap**: past `OUTBOUND_BUFFER_MAX_RECORDS` the oldest records are dropped.
  The ERROR is published at most once every 10 minutes per buffer, with the
  number dropped since the last report.
  - A push appends and applies the cap in one script. A running flush holds
    the lock and trims the head by the count it read, so a trim under it
    would drop unsent records. While the lock is held the push skips the cap.
  - The flush applies the deferred cap when it releases the lock, still
    holding its token, whether it succeeded or failed.
  - Both count what they drop in the buffer's `.dropped` key, and the report
    takes that count.
- **Left-behind buffers**: when a type leaves batch mode or is deselected,
  `flush_buffers` drains what is left. It sends the records with the type's
  current settings if it still has a URL, even when the type was deselected:
  the records were accepted while it was selected. Otherwise it drops them with
  an ERROR. In single mode the drain reads and trims one record at a time, so a
  failure mid-drain never re-sends records already delivered. An invalid `deliver` config
  stops flushing; that is logged as a WARNING at most once an hour.

### Bundles (`deliver_batch`)

A `GundiBatchDelivery` carries many payloads from one provider. Its
`batch_id` is **required**: it is the redelivery dedup key, and a default
would mint a new id on every redelivered parse. The payloads are transformed
(`apply_transformations`), grouped by output type, and:

- batch mode: split into chunks of `max_batch_size` (30 records with a limit of 10 → 3 requests;
  10 with a limit of 20 → 1), one request per chunk, sent directly (no buffer);
- single mode: one request per record.

Redelivery dedup (`app/services/batch_progress.py`, ported from the ER
dispatcher): one Redis record per (batch_id, integration, output type) holds
a fingerprint and a bitmap of **settled requests** (delivered, or permanently
rejected and logged). Bits index requests, not records, because a generic
endpoint gives no per-record result. The fingerprint binds the ordered
`gundi_id`s and the chunk size, so a config change between attempts
invalidates the record. The bundle is then re-sent in full (duplicates, never
loss). Progress is written after every request, so after a retryable failure
the redelivered bundle resumes at the failed request. Records expire after
25 h. A Redis failure reads as "nothing settled".

## OAuth2 effort estimate

- **Client credentials** (machine-to-machine): ~1–2 days, connector only. Add
  `token_url`, `client_id`, `client_secret` (secret), `scope` to Authenticate.
  Fetch and cache the token in Redis until shortly before `expires_in`. On a
  401, refresh it once. No platform changes.
- **Authorization code** (a user grants access): multi-week and cross-repo.
  It needs a portal redirect/consent UX and a callback endpoint, plus
  per-integration storage and refresh of user tokens in cdip (encrypted,
  rotated), and revocation handling. The connector part is small next to the
  portal and cdip work.

## Follow-up

1. **Move `GundiBatchDelivery` into gundi-core** (`gundi_core/events/delivery.py`,
   next to `GundiDelivery`). Keep the class name: the runner routes on
   `event_type`. Keep `batch_id` required.
2. **cdip-routing**: in `app/services/event_handlers.py` (~line 565), the
   batch handler splits bundles into one `GundiDelivery` per item for
   generic-model destinations. Change it to forward one `GundiBatchDelivery`
   per (destination, provider) with the batch's `batch_id`, splitting only
   (never merging) as for ER batches. Until then, this connector only receives
   single deliveries, and `deliver_batch` is reachable only from tests and
   manual publishes.
3. Destinations must have `additional.generic_model = true`.
4. **Template (gundi-integration-action-runner)**:
   - `push_data` returns the runner's response as is, so every non-2xx
     (including a 404 when no `deliver` row exists, or a 422 for a config that
     fails to parse) makes Pub/Sub redeliver until expiry. It should ack
     non-retryable failures the way `/` already does (`_should_redeliver`), or
     route them to a dead-letter topic. This connector avoids the cases it
     controls; that one is the runner's.
   - Upstream `StoredConfigOptionalMixin` and its runner hook, so forks stay mergeable.

## Known limitations (not addressed in the POC)

- **DNS rebinding (TOCTOU)**: the URL is resolved and checked, then httpx
  resolves it again to connect. Closing this needs a transport pinned to the
  checked address, or egress restrictions at the infrastructure level.
- **Module placement**: `jq_transform`, `outbound_buffer` and `batch_progress`
  are new files under the template's `app/services/`. They are additive, so
  merge risk is low, but connector-specific code would normally live in
  `app/actions/`.
- **No connection reuse**: every request opens a new HTTP client, so a
  single-mode bundle of N records pays N TLS handshakes. `send_json` already
  accepts a client, so a bundle or flush could share one.
- **No per-record jq fallback** in batch mode (see JQ semantics): a chunk whose
  filter fails is dropped whole rather than retried record by record.
- **The buffer's Lua scripts are not exercised against a real Redis.** The
  tests emulate them in `app/services/tests/fake_redis.py`. Add a
  `fakeredis[lua]` or real-Redis test before production.
- **URLs behind a password widget**: keeping endpoint URLs secret means users
  cannot read back the URL they saved, and browsers may offer password
  autofill on those fields. A host-only preview would help; check it in the
  portal.
- **Failure wording**: delivery failures go through the template's
  `format_error_message`, which reads "Unexpected response from the provider"
  even though the failing side here is the destination endpoint. A
  destination-specific title would need a change in `app/services/errors.py`.
