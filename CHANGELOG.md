# Changelog

## 0.3.6 — Unreleased

Maintenance release on the 0.3 line. The Kafka recovery baseline remains Message
Store schema version 1. No Message Store schema migration is required. The
optional Key-Value Store uses the `bip_key_value` table, provisioned by the
database operator, only when an application uses it. Tested with Quarkus 3.32.2 (Camel 4.18.0) and
Quarkus 3.33.4 (Camel 4.18.4). This is the last 0.3 release; later changes come
with 0.4.

### Fixed

- A Kafka poll failure reconnects and polls the same position again
  (`camel.component.kafka.poll-on-error=RECONNECT`). Before, a missing read
  permission recorded a `FAILED` row without a record and stopped the consumer
  for good while the process stayed up, and another poll failure could move past
  the offset. An authentication failure still stops the consumer.
- The `x-bip-*` headers of a Kafka record, such as `x-bip-message-id` and
  `x-bip-correlation-id`, are kept as UTF-8 text before the route runs,
  independent of the payload charset. HTTP receivers get the same ids as the
  journal instead of byte-array text such as `[55, 101, …]`. A Kafka publish
  sends the same bytes as before. A receiver that stored the byte-array form
  as a key sees a different value from the upgrade on.
- Kafka record headers are read under both Camel names: `kafka.TOPIC`,
  `kafka.PARTITION`, `kafka.OFFSET` and `kafka.RECORD_META` (Camel 4.18.0), and
  `CamelKafkaTopic`, `CamelKafkaPartition`, `CamelKafkaOffset` and
  `CamelKafkaRecordMeta` (later 4.18.x releases). On the later releases, the
  correlation fix above and the activity counters were otherwise inactive.
  Offset commit and DLQ parking were not affected.
- Credentials in a step's endpoint URI are masked in the Message Store and in
  warnings, the way Camel masks them in its logs. Step identities are unchanged.
  Rows recorded before the upgrade are not rewritten.
- Credential-like header values in DLQ records are masked as `xxxxxx`:
  `Authorization`, cookies, authentication challenges, and names containing
  secret, token, password, credential, session, signature or auth, or ending in
  `-key` or `_key`. Before, request credentials set by a route and response
  headers from a successful receiver call reached the shared DLQ in clear text.
  Header names, `x-bip-*` and `Idempotency-Key` are unchanged.
- A tracked send through Camel's `http` component no longer forwards
  credential-like headers that still hold a consumed Kafka record's bytes, by
  the same name rule. An HTTP intake that keeps the caller's headers publishes
  the caller's `Authorization` with the message. The next route then sent it
  to its own receiver as byte-array text (`[66, 101, …]`), which decodes to the
  credential. The adapter logs a warning once per step and header name,
  without the value. The route keeps the header after the send. A credential
  header the route sets itself is text and is sent as before. A `toD` that
  chooses its component only when it sends, such as `${header.receiverUrl}` or
  `language:header:receiverUrl`, is treated as an HTTP send. Other HTTP components, such as
  `vertx-http`, are unchanged.
- A Kafka send that published a header with a credential-like name, by the same
  rule, logs a warning once per route and header name. The warning names the
  header, never its value, and says how to remove it. The record is published as
  before. Camel's Kafka producer publishes every header that its header filter
  strategy lets through, so an HTTP intake that keeps the caller's headers
  publishes the caller's `Authorization` with the message. The check runs after
  a successful `to` or `toD`, against the endpoint the send used, follows its
  header filter strategy and counts only text, byte array, `Long`, `Integer`,
  `Double` and `Boolean` values.
- A replay's `x-bip-redo-steps` stays inside the integration. When an exchange
  enters a route, the adapter reads the list, keeps it for the attempt's step
  tracking and removes the header, so no send from the route carries it: `to`,
  `toD`, `enrich`, `wireTap`, `recipientList` and `routingSlip`, nor a DLQ
  record. A route can no longer read the header. Before, a receiver got the
  list, and a downstream integration that consumed a published record with the
  same message id sent its own steps with the same ids again. `x-bip-replay` and
  `x-bip-attempt` are still forwarded. The warning for a malformed list no
  longer logs its value.
- When it creates a route, the adapter logs a warning for each HTTP timeout
  option that Camel 4's `http` component accepts without applying it, and names
  the replacement. The call otherwise waits for the component's timeout, three
  minutes unless configured. Checked on Camel 4.18.0 and 4.18.4:
  `socketTimeout`, a Camel 3 option, is sent to the receiver as a query
  parameter; `httpConnection.*` applies only `soTimeout` and drops other
  timeouts; and on a `toD`, an `enrich` or a `wireTap` whose URI contains
  `${…}`, Camel's optimised send also sends `responseTimeout`, `connectTimeout`,
  `connectionRequestTimeout` and `soTimeout` to the receiver. Use the
  `httpClient.` forms or `httpConnection.soTimeout` there. The warning appears
  once per route, step and option and names no URI or value. Forms that fail on
  their own, such as `httpClient.socketTimeout` or
  `camel.component.http.socket-timeout`, are not reported.
- An outcome from an older attempt that arrives late cannot replace a newer
  attempt's message summary. This includes the original Kafka record consumed
  again as attempt 1 after a replay recovered it, which used to reset the
  latest attempt to 1.
- A replay can no longer keep an inline payload indefinitely. Each attempt still
  stores a new expiry of capture time plus retention, so a replay resets
  retention, but never beyond first receipt plus twice the retention. First
  receipt is the earliest outcome this integration recorded for the message.
  Once the expiry has passed, replay is refused as for any expired payload.
  Every history row keeps its own expiry, including a late older attempt, and
  metadata-only attempts have none. The optional message recovery SQL, when
  installed, applies the same cap in the database, so a replay is capped at first
  receipt plus twice the retention with or without it. The database takes the
  retention from the first captured attempt; if the retention setting changed
  since then, the lower cap applies. The database-side recovery protections
  need separate provisioning; do not assume an adapter upgrade deploys them.
- A route that replaces the message without the `x-bip-*` headers, such as a
  split with a grouped aggregation strategy or `removeHeaders("*")`, no longer
  records the outcome under a new random message id. The message id,
  correlation, attempt and envelope fall back to the consumed input's values; a
  value the route sets itself still wins.
- A failure sent to the dead letter queue is parked and recorded under the
  consumed input's message id. Before, a route that set another id for a send to
  another integration, and failed before restoring it, parked and recorded the
  failure under that downstream id.
- Logs no longer suggest a delivery that did not happen. A step skipped because
  an earlier attempt delivered it is logged at INFO. The logging sink counts
  delivered, skipped and failed steps separately. Routes can read
  `basis.messageStore.stepAlreadyDelivered` in the step after a send.
- A failure that is sent to the dead letter queue is logged at ERROR with its
  stack trace first.
- A send whose step outcome cannot be checked or recorded, for example during a
  Message Store outage, fails its exchange. The input is then neither parked
  nor committed, and is recorded as `FAILED`, so it is consumed again. This
  also holds when the route catches the failure and when it happens in a split
  part, loop iteration or branch. Before, when the outcome write failed after a
  send that completed asynchronously, such as a Kafka publish, the exchange
  never completed. A send that completed synchronously hung the same way in a
  route with no error handler or inside `doTry`, and in a split part it could
  let the input complete and commit. Camel's retries still run before the
  input fails, so a receiver can get the same send again; receivers deduplicate
  on the stable `Idempotency-Key`. A failed send keeps its own exception, with
  the recording failure attached as suppressed. A separate request, such as one
  sent with a `ProducerTemplate` or recorded by delivery audit, likewise fails
  when one of its step outcomes is not recorded, also when its own route caught
  the failure. An input that passes that failure on is neither parked nor
  committed either.
- The DLQ record carries the consumed input, also when the route changed it.
  Before, a route that changed a byte-array input in place changed both the
  parked record and the payload stored for replay. When the payload was not
  stored for replay, in `metadata-only` mode, with replay disabled or above
  `basis.message-store.payload.max-bytes`, a route that transformed the body
  and then failed parked the transformed body. The adapter now keeps one copy of
  the input at route start: the replay copy when the payload is stored, and
  otherwise a copy for parking, only for a Kafka input. A String input is not
  copied. A body of another type, such as a stream, is still parked as the
  route left it. A missing body is parked as an empty payload, as with inline
  storage.
- An application with more than one `@Default` JDBC DataSource no longer starts
  with the Postgres Message Store or step store. A second DataSource from a
  `@Named` producer is also `@Default`. Before, the application started and
  reported ready, and every message then failed with "requires a JDBC
  DataSource". The error now says that the lookup is ambiguous. Give the
  additional DataSource a qualifier, such as `@Identifier("<name>")`.

### Splitting before a tracked send in 0.3.x

In 0.3.x a step key is `<x-bip-message-id>:<step-id>`. Split parts, loop
iterations and multicast and recipient list branches keep the input's message
id, so parts that reach the same tracked send share one key. With the Postgres
step store, only the first part is sent. The later ones are skipped as already
delivered and their data does not reach the receiver, while the input completes:
a Kafka offset is committed, or a polling route goes on to mark its batch
processed. Version 0.3.5 and earlier log nothing for these skips.

From 0.3.6, in the attempt that sends the first part, each such skip is logged
at ERROR, once per skipped send:

```text
Step <step-id> was skipped in a <split part|loop iteration|multicast branch|recipient list branch>
of input route <route-id>, so this part was not delivered: another part of the same attempt
already sent this step key. In 0.3.x split parts, loop iterations and multicast and
recipient list branches keep the input's x-bip-message-id, so they share one step key. Give
each part its own x-bip-message-id before the send, or publish each part with its own id. See
docs/split-step-identity.md
```

- The message is one line and contains no message content or header values.
- Only the log changes. The skip itself, the input's outcome, retries, DLQ
  parking, offset commits and the recorded step outcomes are as in 0.3.5.
- The line is written only when a part at another position sent the key in
  this attempt and this part did not. Every other skip stays at INFO. That
  includes every part of a redelivered or replayed input whose earlier attempt
  sent the key, and a part that meets its own delivered send again when Camel
  repeats a sub-route or a whole split.
- The `noop` step store never skips, so every part is sent and nothing is
  logged.
- Not detected, and still logged at INFO only: see the advisory below for the
  exact list, such as a split that runs again in the same attempt and a send
  after an inner loop in an outer loop.

The safe forms, which send every part:

1. Give each part its own `x-bip-message-id` before the send, derived from its
   record so that a replay keeps the same key.
2. Publish each part with its own id, for example as a Kafka record to the
   integration's own input, and send it from there.

A part sent through a new exchange, for example with a `ProducerTemplate`, is
not a safe form on its own. Sent straight to the receiver's endpoint, the send
is not tracked: it has no step key, the receiver gets no `Idempotency-Key`, and
a replay sends it again. Sent to a route that contains the tracked send, such as a
`direct:` route, it is tracked under the new exchange's `x-bip-message-id`.
Without its own id, each send then gets a new random id, so a replay sends it
again too. In neither case is the part journaled, and the input's journal row
does not list its send.

After a route changes to per-part ids, its keys change: replaying a message
recorded before the change sends its first part once more. 0.4 gives each part
and iteration its own step key. See the
[split step identity advisory](docs/split-step-identity.md).

### New and opt-in

- Key-Value Store: an integration-scoped API for small JSON cursors and markers,
  with versions, compare-and-set, optional expiry, bounded cleanup and a Camel
  idempotent repository. It is optional, uses the `bip_key_value` table
  provisioned by the database operator, and never creates or migrates tables.
  See [Key-Value Store](docs/key-value-store.md).
- Activity counters for admitted Kafka records and broker-acknowledged
  publications, through the application's Micrometer registry. Off by default;
  incomplete coverage is reported as unknown, not as zero. See
  [activity metrics](docs/activity-metrics.md).

### Known limitations

- A step outcome that is not recorded holds back the input only while the input
  waits for it. Work that ends after the input completed, such as a `wireTap`,
  fails only its own exchange, and a route that ignores a separate request's
  failure lets its input complete. Such work needs its own recovery.
- Split parts and loop iterations that send through the same node share one
  step identity. With the Postgres step store, only the first is delivered and
  the rest are skipped, also in the first attempt. From 0.3.6 such a skip is
  logged at ERROR in the attempt that sends the first part (see
  [Splitting before a tracked send in 0.3.x](#splitting-before-a-tracked-send-in-03x));
  nothing else changes. Without `stopOnException()`,
  each failed part is parked separately while the input records `COMPLETED`. See
  the [split step identity advisory](docs/split-step-identity.md). This applies
  to every 0.3.x release. Fixed in 0.4, which gives each part and iteration its
  own step key.
- The Message Store is keyed by message id alone. A Kafka input adopts an
  incoming `x-bip-message-id`, and Camel's Kafka producer forwards it by default.
  When a message id reaches a second integration on the same Message Store, the
  two integrations collide. The second integration replaces the first one's
  journal rows, so the message moves to the other integration's view and can no
  longer be replayed by the first. With the optional message recovery SQL
  installed, the second integration's journal write is refused instead, and its
  input does not get past that record. A send with the same step id is skipped as
  already delivered. This applies to every 0.3.x release. From 0.3.6 the adapter
  logs a warning, once per pair of integrations and then at most every ten
  minutes with a count, when it journals a message id that another integration
  already journaled. With the message recovery SQL, or row-level
  isolation between integrations, that warning does not appear: the signal is
  the refused write itself (`RECOVERY_SCOPE_MISMATCH` or a row-level security
  error) in the second integration's log. Fixed in 0.4, where each integration
  derives its own message id by default.
- An invalid Kafka authentication stops the consumer for good while the process
  keeps running. A receiver can apply an effect and lose its response, so retries
  can repeat it; receivers must deduplicate on the stable `Idempotency-Key`.
- Key-Value Store versions belong to a physical row: after expiry cleanup or
  deletion and recreation, the version starts at 1 again and a stale numeric
  compare-and-set can match. It has no leases or fencing.
- The replay expiry cap counts from the earliest outcome still recorded for the
  message. Recording the same attempt again replaces that attempt's outcome
  time, for example when the original record is consumed again after a replay.
  If it was the earliest outcome, the adapter's cap for a later replay starts
  from the next earliest one, so it can reach past first receipt plus twice
  the retention by up to the time between those two outcomes.
- DLQ records still carry other headers present at failure, such as a
  receiver's host names. Only credential-like values are masked.
- `auto_redelivery_count` counts Camel's redeliveries. The HTTP client in
  Camel's `http` component repeats a request answered with 503 or 429 once by
  itself, POST included, so a receiver can see more requests than the count
  says. Before that retry it waits for the receiver's `Retry-After`, with no
  upper limit, or one second without it. The wait runs on the Kafka consumer
  thread and counts against `max.poll.interval.ms` (300 seconds by default);
  see the processing-time budget under [Retry and DLQ](README.md#retry-and-dlq).
  Setting `camel.component.http.automatic-retries-disabled=true` leaves every
  retry to the adapter's retry policy. Set it for receivers that send a long
  `Retry-After` and for routes on `ip-slow-backend`.
- The HTTP timeout warning reads `to`, `toD`, `enrich` and `wireTap` URIs when
  a route is created. A URI in a `recipientList` or `routingSlip`, or one whose
  scheme is only known at send time, is not checked. See the README's retry
  section for the full table.
- The Kafka credential warning does not check an endpoint whose header filter
  strategy or header serializer is an application class, or whose filter holds
  an application collection, nor a send through a `toD` with `cacheSize=-1` to
  an endpoint that is not yet registered, which Camel creates for that send
  only. It reports at most 100 header names per route.
- A handler that restores the original message, for example with
  `useOriginalMessage()`, gets `x-bip-redo-steps` back until the route's next
  `to` or `toD` or DLQ parking removes it.

### Upgrade notes

- **Security: rotate credentials that may be in DLQ records.** Version 0.3.5 and
  earlier wrote the values of request credential headers, such as
  `Authorization` and API keys, in clear text to DLQ records. Rotate credentials
  that may be in existing DLQ topics, and restrict who can read DLQ topics.
- **Every tracked external send needs an explicit id.** An application whose
  routes contain a `to` or `toD` to an external endpoint without an `id` no
  longer starts; the error names the route and the endpoint. Generated ids such
  as `to1` change when a route is edited and repeat across integrations, which
  can make one integration skip another's send. Add explicit ids before
  upgrading. To keep each send's key, use its current generated id as the
  explicit id, for example `.id("to1")`; replays of earlier messages are then
  still skipped. Generated numbers count across all routes, including the
  adapter's dead letter route, so confirm them first: start the unchanged
  application on 0.3.6 in a test environment with the emergency setting below,
  which logs each send's generated id, and compare with the step ids recorded
  in `bip_step_outcome`. The README's step tracking section has the query.
  Renaming a generated id, for example `to1` to `send-to-sap`, changes that
  step's key: replaying, redoing or resending a message recorded before the
  change sends it once more. Rename only once no such message will be processed
  again. The startup check covers the routes as Camel builds
  them, so it also sees endpoints whose URIs are built elsewhere, such as
  placeholders and constants, which a check of the route source misses. Only in an
  emergency, `basis.message-store.step-ids.emergency-allow-generated=true` starts
  the application anyway and logs a warning for each such send.
- Add the Camel health bridge (`camel-quarkus-microprofile-health`). With
  SmallRye Health alone, readiness stays
  Ready after the Kafka consumer stops. With the bridge, readiness turns
  NotReady during a broker outage or a stopped consumer, and Ready again after
  reconnection. NotReady does not restart a pod: alert on sustained NotReady.
- Set the readiness probe timeout to 3–5 seconds. While the consumer reconnects,
  Camel's Kafka health check can take more than 2 seconds to answer.
- **Look for the new warnings** `HTTP option … has no effect` at startup and
  `Kafka send … published header …` while messages flow. Use the replacement the
  first one names; remove the header the second one names before the send.
- **Look for the new ERROR line `Step <step-id> was skipped in a …`.** Each one
  is a split part, loop iteration or branch that was not delivered. On 0.3.5 the
  same parts were skipped without a log line. Move the route to one of the safe
  forms in
  [Splitting before a tracked send in 0.3.x](#splitting-before-a-tracked-send-in-03x).
- Give each integration its own `x-bip-message-id`: set a new one before
  publishing to another integration's input, and carry continuity in
  `x-bip-correlation-id`. Explicit step ids must also be unique across the
  integrations that can see the same message id.
- Tools that read DLQ records see `xxxxxx` instead of credential header values.
- A byte-array Kafka input now holds one extra copy of its size until its
  exchange completes, in every payload mode. A String input does not.
- The journal writer works with and without the optional message recovery
  SQL. With it installed, the recovery history protection, payload expiry clamp
  and replay refusals apply as before. No grant changes are needed.
- See [maintenance qualification](docs/maintenance-qualification.md) and
  [delivery audit](docs/delivery-audit.md) for recovery and inspection details
  carried over from 0.3.5.
- The startup messages about the store schema version and the Key-Value Store's
  missing-table error are reworded. The `MESSAGE-STORE CONTRACT VERSION
  UNVERIFIED` prefix is unchanged.

## 0.3.5 — 2026-10-12

Maven artifact:
`no.basis.integrationplatform:integration-platform-adapter-camel-quarkus:0.3.5`.
The source version is `0.3.5-SNAPSHOT`; a snapshot is not this stable release.
The Kafka recovery baseline remains Message Store schema version 1.
Publication, qualification and application rollout are separate gates.

### Added

- Opt-in `basis.delivery-audit.routes`: exact `direct:` request route ids retain
  outgoing request bytes and terminal outcomes with distinct message ids and the
  supplied original correlation id. The default empty selection preserves
  0.3.4 recording behavior. See [delivery audit](docs/delivery-audit.md).
- Opt-in `basis.input-audit.routes`: inspect selected non-Kafka input boundaries
  without assigning Kafka replay, offset commit or DLQ ownership. Input headers
  remain unchanged, including absent ids and poll/batch producer identities.
  See [input inspection](docs/input-audit.md).

### Fixed and preserved

- Receiver and audit-journal failures propagate to the original input's recovery
  handler. Recording an audit never claims successful delivery or commits an
  input offset; failed durable input recording still prevents offset commit.
- Audit payloads are always non-replayable. Delivery audit cannot resend
  transformed receiver bytes into the Kafka input, even when those bytes are
  retained or replay is enabled.
- Route selectors are separate from deployment-owned `basis.message-store.*`
  settings. Startup rejects unknown, wildcard, overlapping or unsafe inherited
  identity/capture/commit selections before processing.

### Upgrade actions and limitations

- No Message Store schema migration or new service is required. Existing
  integrations may use the same version with both selectors empty.
- Review route ids, source acknowledgement/recovery and header handling before
  opting in. Each delivery audit needs a fresh `ProducerTemplate.request`
  exchange carrying only the required correlation/content/receiver headers.
  A same-exchange `to("direct:...")` child is not a delivery audit boundary.
- Non-Kafka input inspection retains visibility only; it does not implement
  source-specific retry, acknowledgement or recovery. Streams/file wrappers
  remain metadata-only. Poll inspection is not per-record batch inspection.
- Replay the original input to recover a delivery. This may repeat external
  calls; audit ids are not business-level deduplication. Route success does not
  independently prove receiver business acceptance.
- Existing payload mode, inline size cap and retention still apply. There is no
  historical row backfill. Applications with no adapter need a separate
  integration decision, not an automatic dependency insertion.
- Operator searches must support correlation ids. Operational failure counts
  must distinguish delivery audit history from source outcomes; adapter upgrade
  alone does not change how an older operator view counts them.

See the [configuration reference](docs/configuration.md) for defaults and
[maintenance qualification](docs/maintenance-qualification.md) for the Kafka
baseline and release provenance requirements.

## 0.3.4

- Restrict automatic Message Store capture/replay boundaries to Kafka inputs;
  internal `direct:` and HTTP receiver exchanges no longer become replay inputs.
- Keep infrastructure DLQ parking outside delivered-step skipping and preserve
  original receiver failure diagnostics.
- Require confirmed durable failure parking before Kafka offset commit. Add
  startup and maintenance acceptance fixtures for these recovery boundaries.

## 0.3.3

- Apply configured Kafka TLS/SASL authentication to adapter-owned parking and
  replay producers, matching the application's Kafka security configuration.
  Hostname verification remains enabled; existing plaintext is the default.

## 0.3.2

- Exclude XSLT transformations from delivered-step tracking so replay repeats
  transformation work instead of treating it as a delivered external effect.
- Refresh the README.

## 0.3.1

First public release, on Maven Central. Functionally identical to the
internally qualified 0.3.0 (message-store schema version 1); this release
adds only licensing, publication metadata and packaging. Versions below 0.3.1
were internal to Basis.
