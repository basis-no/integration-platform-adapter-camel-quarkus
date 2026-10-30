# Changelog

## 0.3.6 — Unreleased

Maintenance release on the 0.3 line. The Kafka recovery baseline remains Message
Store contract `v1.0.0-rc2`. No Message Store schema migration is required. The
optional Key-Value Store uses the platform-provisioned `bip_key_value` table
only when an application uses it. Tested with Quarkus 3.32.2 (Camel 4.18.0) and
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
  metadata-only attempts have none. The platform's optional message recovery
  SQL applies the same cap in the database, so a replay is capped at first
  receipt plus twice the retention with or without it. The database takes the
  retention from the first captured attempt; if the retention setting changed
  since then, the lower cap applies. The message recovery SQL is not part of
  the next platform release.
- A route that replaces the message without the platform headers, such as a
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

### New and opt-in

- Key-Value Store: an integration-scoped API for small JSON cursors and markers,
  with versions, compare-and-set, optional expiry, bounded cleanup and a Camel
  idempotent repository. It is optional, uses the platform's `bip_key_value`
  table from draft contract `1.2.0-rc2`, and never creates or migrates tables.
  See [Key-Value Store](docs/key-value-store.md).
- Activity counters for admitted Kafka records and broker-acknowledged
  publications, through the application's Micrometer registry. Off by default;
  incomplete coverage is reported as unknown, not as zero. See
  [activity metrics](docs/activity-metrics.md).

### Known limitations

- Split parts and loop iterations that send through the same node share one
  step identity. With the Postgres step store, only the first is delivered and
  the rest are skipped, also in the first attempt. Without `stopOnException()`,
  each failed part is parked separately while the input records `COMPLETED`. See
  the [split step identity advisory](docs/split-step-identity.md). This applies
  to every 0.3.x release. Fixed in 0.4, which gives each part and iteration its
  own step key.
- The Message Store is keyed by message id alone. A Kafka input adopts an
  incoming `x-bip-message-id`, and Camel's Kafka producer forwards it by default.
  When a message id reaches a second integration on the same Message Store, the
  two integrations collide. The second integration replaces the first one's
  journal rows, so the message moves to the other integration's view and can no
  longer be replayed by the first. With the platform's message recovery SQL
  installed, the second integration's journal write is refused instead, and its
  input does not get past that record. A send with the same step id is skipped as
  already delivered. This applies to every 0.3.x release. From 0.3.6 the adapter
  logs a warning, once per pair of integrations and then at most every ten
  minutes with a count, when it journals a message id that another integration
  already journaled. With the platform's message recovery SQL, or row-level
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
- DLQ records still carry other headers present at failure, such as a
  receiver's host names. Only credential-like values are masked.

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
  them; the platform's pre-build check reads the source and misses endpoints
  whose URIs are built elsewhere, such as placeholders and constants. Only in an
  emergency, `basis.message-store.step-ids.emergency-allow-generated=true` starts
  the application anyway and logs a warning for each such send.
- Add the Camel health bridge (`camel-quarkus-microprofile-health`); the
  platform template includes it. With SmallRye Health alone, readiness stays
  Ready after the Kafka consumer stops. With the bridge, readiness turns
  NotReady during a broker outage or a stopped consumer, and Ready again after
  reconnection. NotReady does not restart a pod: alert on sustained NotReady.
- Set the readiness probe timeout to 3–5 seconds. While the consumer reconnects,
  Camel's Kafka health check can take more than 2 seconds to answer.
- Give each integration its own `x-bip-message-id`: set a new one before
  publishing to another integration's input, and carry continuity in
  `x-bip-correlation-id`. Explicit step ids must also be unique across the
  integrations that can see the same message id.
- Tools that read DLQ records see `xxxxxx` instead of credential header values.
- The journal writer works with and without the platform's message recovery
  SQL. With it installed, the recovery history protection, payload expiry clamp
  and replay refusals apply as before. No grant changes are needed.
- See [maintenance qualification](docs/maintenance-qualification.md) and
  [delivery audit](docs/delivery-audit.md) for recovery and inspection details
  carried over from 0.3.5.

## 0.3.5 — 2026-10-12

Maven artifact:
`no.basis.integrationplatform:integration-platform-adapter-camel-quarkus:0.3.5`.
The source version is `0.3.5-SNAPSHOT`; a snapshot is not this stable release.
The Kafka recovery baseline remains Message Store contract `v1.0.0-rc2`.
Publication, platform certification and application rollout are separate gates.

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
  retained or the platform enables replay.
- Route selectors are separate from platform-owned `basis.message-store.*`
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
- Dashboard searches must support correlation ids. Operational failure counts
  must distinguish delivery audit history from source outcomes; adapter upgrade
  alone does not change an older dashboard's counting behavior.

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
- Refresh the README and link the public Message Store contract.

## 0.3.1

First public release, on Maven Central. Functionally identical to the
internally certified 0.3.0 (message-store contract v1.0.0-rc2); this release
adds only licensing, publication metadata and packaging. Versions below 0.3.1
were internal to the Basis platform.
