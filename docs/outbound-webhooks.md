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
| `get_data_samples` | reference | portal (on demand) | Returns the captured data samples for the transformation editor (see [Data samples](#data-samples-for-the-transformation-editor)) |

Code: `app/actions/handlers.py`, models in `app/actions/configurations.py`,
HTTP client in `app/actions/client.py`, bundle envelope in
`app/actions/envelopes.py`, jq editor annotation in `app/actions/jq_editor.py`,
and in `app/services/`: `jq_transform.py`, `outbound_buffer.py`,
`outbound_samples.py` and `batch_progress.py`.

There is no scheduled action: buffers are flushed inline by the deliveries
(see [Batch mode](#batch-mode-deliver--buffer)). A scheduled action would not
help anyway: cdip schedules pull actions only for integrations used as
providers, so it would never run for a destination-only integration. It would
also run every minute for every inbound webhook provider, and make the type
look like a data source in the portal.

`deliver` and `deliver_batch` publish activity events on **errors only**.
cdip-routing sends one `GundiDelivery` per record, so started/completed
events would add two feed entries per record. `get_data_samples` publishes
none: the portal calls it whenever the editor opens or refreshes.

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
- There is no "Test Connection" button (the action is not executable): an
  endpoint can only be tested by sending it data. The `auth` action still
  exists, following the template's convention.

**Deliver**

- `endpoints`: a list, one item per data type to deliver. A type with no
  endpoint is not delivered. Records of that type, and anything else routed
  here (e.g. attachments), are dropped with an INFO entry in the activity
  log. An empty list, or the `{}` row cdip creates for every action, delivers
  nothing. Each item has:
  - `output_type`: Observations, Events, Event Updates or Messages (stored as
    `observation`, `event`, `event_update`, `message`). **Only one endpoint per
    data type**: a duplicate is rejected on save.
  - `url` (required, shown as plain text): webhook URLs are often
    credentials (Slack-style hook paths, `?token=`). The model types it as
    `SecretStr`, so it never appears in activity-log config data or error
    events, but the portal renders it as a text input so users can read back
    what they saved.
    - The JSON schema has `pattern: "^https://"`, so the portal and cdip refuse
      a non-https URL on save, and the model applies the same rule.
    - At send time the URL must also resolve to public addresses only
      (`app/services/url_policy.py`). It is checked before every request, and
      redirects are not followed. Set `OUTBOUND_URL_ALLOWLIST` to restrict
      hosts further.
  - `method`: `POST` (default) or `PUT`.
  - `jq_filter`: default `.`.
  - `batch_mode` (default off), `max_batch_size` (default 100, ≥ 1),
    `max_wait_seconds` (default 60, ≥ 60).
  - `capture_samples` (default off): keep recent records for the
    transformation editor (see [Data samples](#data-samples-for-the-transformation-editor)).

  The list renders like `custom_headers`: an Add button, with an explicit
  `ui:order` inside the items.

Settings (`app/settings/integration.py`):

| Setting | Default |
|---|---|
| `OUTBOUND_REQUEST_TIMEOUT_SECONDS` | 30 |
| `OUTBOUND_URL_ALLOWLIST` | empty: any public host |
| `OUTBOUND_FLUSH_LOCK_SECONDS` | 300; must exceed 2 × the request timeout, checked at startup |
| `OUTBOUND_BATCH_PROGRESS_TTL_SECONDS` | 25 h |
| `OUTBOUND_BUFFER_MAX_RECORDS` | 10000 |
| `OUTBOUND_BACKOFF_INITIAL_SECONDS` | 30 |
| `OUTBOUND_BACKOFF_MAX_SECONDS` | 900; must be ≥ the initial backoff |
| `OUTBOUND_OVERFLOW_REPORT_SECONDS` | 600: buffer-overflow ERRORs at most this often |
| `OUTBOUND_SAMPLES_MAX` | 3: data samples kept per data type |
| `OUTBOUND_SAMPLES_TTL_SECONDS` | 172800 (2 days) after the last capture |
| `OUTBOUND_SAMPLE_MAX_BYTES` | 65536: a larger stored entry (record plus capture time) is not captured |
| `OUTBOUND_SAMPLES_TIMEOUT_SECONDS` | 1: longest a capture or delete may hold up a delivery |

Every count and duration must be a positive integer, and the timeout a
positive number. `validate_outbound_settings` checks them at import, together
with the relationships noted above, and raises an error naming the setting and
its value.

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
filters defensively, e.g. `map(.x | tonumber? // null)`.

**Line breaks and comments** (same rules for inbound webhook filters, which
share `jq_all` in `app/services/jq_transform.py`): the filter goes to jq as
written, with `\r\n` and `\r` turned into `\n`. So multi-line filters work as
in the jq CLI:

- a line break is whitespace (`if … then .b⏎else .a⏎end`, `and`/`or` at the
  start of a line);
- `#` starts a comment that ends at the line break, so later lines still run;
  a `#` inside a string is just a character;
- a raw line break inside a string literal is a newline in the string.

Line breaks used to be **deleted** before compiling. Filters that compiled
that way give the same output now, except where the deletion changed their
meaning:

- it glued two tokens: `.a⏎and .b` read the field `aand`;
- a `#` comment swallowed every later line, which now run;
- a raw line break inside a string was dropped, and is now kept;
- a line break inside a name (`.fo⏎o`) used to read `.foo`, and is now a
  syntax error.

Multi-line `if/then/else` used to fail to compile, as did `and`/`or` at the
start of a line after a name (`null⏎or` became `nullor`); they now work. CRLF filters always failed, because jq rejects
`\r`; they now work too.

## The transformation editor contract

`DeliverConfig.ui_schema()` gives the portal what its jq editor (Schema &
example, Transformations, Preview) needs, on `endpoints.items.jq_filter`:

```json
{
  "ui:widget": "textarea",
  "ui:options": {"language": "jq", "rows": 4},
  "gundi:jq_transform": {
    "output_type_field": "output_type",
    "batch_field": "batch_mode",
    "samples_action": "get_data_samples",
    "schemas":  {"observation": {}, "event": {}, "event_update": {}, "message": {}},
    "examples": {"observation": {}, "event": {}, "event_update": {}, "message": {}}
  }
}
```

- `output_type_field` and `batch_field` name the sibling fields that choose
  the schema/example and whether the filter's input is one record or an array.
- `schemas` are the pinned gundi-core models' `.schema()` (`Observation`,
  `Event`, `EventUpdate`, `TextMessage`).
- `examples` are built from those models, using gundi-core's example values
  where it has them. They are serialized with `jq_input`
  (`app/services/jq_transform.py`), the same function that builds the
  deliver actions' jq input, so they have exactly the shape a filter receives.
  In batch mode the editor wraps them in an array.
- `samples_action` is the reference action that returns real captured
  records (below).
- The annotation is about 12 KB (a test keeps the whole ui schema under
  40 KB). It reaches the portal through self-registration, so a gundi-core
  bump changes it on the next deploy.

The portal must ignore the `gundi:` key in other rjsf contexts; rjsf itself
ignores keys it does not know.

## Data samples for the transformation editor

With `capture_samples` on, an endpoint keeps the latest records of its data
type, **as the jq filter receives them** (after `apply_transformations`,
serialized with `jq_input`), so the editor can preview a filter against real
data.

- **When**: at the point where the jq input is built, before buffering or
  sending, in `deliver` (single and batch mode) and `deliver_batch`. A record
  is captured even if its delivery then fails.
- **Storage**: Redis list `outbound_samples.{integration_id}.{output_type}`,
  newest first. Each entry is `{"captured_at": "<ISO-8601 UTC>", "record": {...}}`.
  One `MULTI` pipeline does `LPUSH`, `LTRIM` to `OUTBOUND_SAMPLES_MAX`, and
  `EXPIRE` `OUTBOUND_SAMPLES_TTL_SECONDS`. A bundle pushes only its newest
  `OUTBOUND_SAMPLES_MAX` records per type: the trim would discard the rest.
  Entries over `OUTBOUND_SAMPLE_MAX_BYTES` are skipped (debug log).
- **Turning it off** (or removing the endpoint): `get_data_samples` stops
  returning that type's samples at once, and deletes them when called.
  Deliveries delete them too: a record of a type whose endpoint has capture
  off issues a `DEL`, at most once a minute per (integration, type) per
  runner instance, so the default (off) does not add a Redis round trip to
  every record. Otherwise they expire with the TTL. Types without an
  endpoint are never captured. The field description, built from the
  settings, says this.
- **Never fails or stalls a delivery**: a capture or delete gets
  `OUTBOUND_SAMPLES_TIMEOUT_SECONDS`. A failure or a timeout is logged at
  WARNING and the delivery goes on.
- **Duplicates**: a Pub/Sub redelivery captures the same record again.

**Privacy**: samples are real data (locations, sensor readings, message
text) copied into Redis, outside the delivery path. That is why capture is
off by default and per data type, the field's description says so, the list
is short, and the TTL is short. They can be read through `get_data_samples`
by whoever may execute the integration's actions in cdip: superusers and
admins of the integration's organization (viewers cannot POST to the execute
endpoint). Turn capture off when the transformation is done.

`get_data_samples` returns tenant data, and the runner's `/v1/actions/execute`
has no app-level auth: the runner's Cloud Run service must stay restricted by
IAM or ingress so that only cdip and Pub/Sub can call it.

### `get_data_samples` (reference action)

Executed through cdip's
`POST /v2/integrations/{id}/actions/get_data_samples/execute/` with optional
`config_overrides: {"output_type": "event"}`. Like every reference action,
it is registered with type `reference` (hidden from config sections), runs
without a stored config row, and needs no activity logging. It returns:

```json
{"samples": {"event": [{"captured_at": "...", "record": {}}]},
 "capture_enabled": {"observation": false, "event": true, "event_update": false, "message": false},
 "max_samples": 3, "ttl_seconds": 172800}
```

- `samples`: newest first. Only the requested type when `output_type` is
  given, otherwise all four (empty lists when there are none).
- `capture_enabled`: all four types, from the integration's current deliver
  config. `false` for a type with no endpoint, or when the config is missing
  or invalid. A type with `false` always has `[]` samples.
- A draft (ephemeral) run gets a fresh integration id, so it returns no
  samples; `capture_enabled` reflects the draft's deliver config.
- An unknown `output_type` is rejected with 422.
- **Response body**: the runner returns the handler's dict as the HTTP body
  unchanged (`execute_action` returns `result` as is), and cdip's
  `ActionTriggerView.execute` returns the runner's JSON as is. Read
  `body.samples`, not `body.result.samples`. Errors are not passed through:
  cdip calls `raise_for_status()` on the runner's answer, so a runner error
  surfaces as a cdip error rather than the runner's `{"detail": ...}` body.

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
`max_wait_seconds`. Flushes run **only inline**, when a record arrives for
the integration:

- after every `deliver` into a buffer, for that buffer;
- after every `deliver` or `deliver_batch` of any type, a sweep of the
  integration's other buffers. It does one or two cheap reads per buffer
  (length, then the age of the oldest record), and only flushes what is due.
  The sweep respects each buffer's backoff and lock. A failure in it is
  logged and never fails the delivery that triggered it.

**A quiet integration may hold a partial batch until the next record arrives
for that integration**, however long `max_wait_seconds` has passed. Nothing
flushes a buffer on a timer. See follow-up 5.

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
  `OUTBOUND_BACKOFF_MAX_SECONDS`. Flushes and sweeps skip the buffer until the
  delay passes. The WARNING ("will retry in N s") is published once
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
- **Left-behind buffers**: when a type leaves batch mode, the sweep drains
  what is left with the endpoint's current settings. The drain reads
  and trims one record at a time, so a failure mid-drain never re-sends
  records already delivered. When a type's endpoint is removed, there is no
  URL to send to, so its buffered records are dropped with an ERROR listing
  their `gundi_id`s. While the `deliver` config is invalid, no delivery
  runs, so no buffer moves.

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
endpoint gives no per-record result. The fingerprint (a truncated SHA-256
over length-prefixed fields) binds exactly:

- the ordered `gundi_id`s of the type's records;
- the chunk size (`max_batch_size` in batch mode, 1 in single mode);
- the request plan: the endpoint URL, the HTTP method, the JQ filter, and
  batch vs single mode (a batch of 1 sends an array, single mode an object);
- a SHA-256 of the effective headers (names case-folded and sorted, with their
  values: the API key header, custom headers and `Content-Type`), or a fixed
  marker when the auth config is invalid.

Header values only ever enter a hash; nothing secret is stored or logged.
Any change to these between attempts invalidates the record. The bundle is
then re-sent in full: duplicates, never loss. This matters because rejected
requests are recorded as settled, so a fixed URL or rotated credentials make
them go out again. Progress is written after every request, so after a retryable failure
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
5. **Runner-wide scheduled flush sweep**: something that flushes aged buffers
   without waiting for traffic, so a quiet integration's partial batch is
   still sent after `max_wait_seconds`. Per-integration pull actions do not
   fit, because cdip schedules them only for providers. One option is a
   single runner-level periodic job that scans the `outbound_buffer.*` keys
   and runs the same sweep.

## Known limitations (not addressed in the POC)

- **DNS rebinding (TOCTOU)**: the URL is resolved and checked, then httpx
  resolves it again to connect. Closing this needs a transport pinned to the
  checked address, or egress restrictions at the infrastructure level.
- **Module placement**: `jq_transform`, `outbound_buffer`, `outbound_samples`
  and `batch_progress` are new files under the template's `app/services/`. They are additive, so
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
- **Failure wording**: delivery failures go through the template's
  `format_error_message`, which reads "Unexpected response from the provider"
  even though the failing side here is the destination endpoint. A
  destination-specific title would need a change in `app/services/errors.py`.
