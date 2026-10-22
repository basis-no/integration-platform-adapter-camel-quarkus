# Basis Camel Quarkus Adapter

Shared Camel Quarkus adapter for Basis integration platform message handling.

Implements message-store contract v1.0.0-rc2.
The contract applies the platform-wide
BIP naming (ADR-002): Kafka topics `bip.*` (`bip.<integration>.in`,
`bip.message.dlq`, `bip.message.status`), Postgres tables `bip_*`
(`bip_message_log`, `bip_message_attempt`, `bip_message_replay_audit`,
`bip_step_outcome`, `bip_contract`), and envelope headers `x-bip-*`.
Environment variables (`BASIS_MESSAGE_STORE_*`) and config keys
(`basis.message-store.*`) keep their prefixes; only values that name topics,
tables, or headers use the `bip` names.

The adapter keeps platform mechanics out of business routes where possible:

- standard `x-bip-*` envelope headers
- message attempt/status collection
- step tracking and idempotency-key injection around endpoint sends
- retry/DLQ route configuration defaults
- Kafka manual offset commit after the message outcome is safely recorded
- a small `MessageStoreSink` boundary so persistence can be direct Postgres now
  or lifecycle events/central message-store service later

This library is a Camel/Quarkus runtime adapter. It is self-contained: the
tables, topics and headers it reads and writes are documented in this README,
and the schema it expects ships in `src/main/resources/db/message-store/`.

See the [changelog](CHANGELOG.md) for candidate changes, upgrade actions and
limitations. [Release tooling](docs/release-packaging.md) builds unsigned candidates
without production credentials. Production signing and snapshot publication are
suspended pending an enforceable isolated signer and credential review. A tag
never rebuilds or uploads a release. Final notes need an approved date and exact
artifact provenance; source preparation does not authorize publication.

## 0.3.5 maintenance line

This maintenance line retains the 0.3.4 replay and failure-ownership safeguards.
Its runtime source passed the contract-v1
cross-runtime suite and isolated runtime acceptance. See
[0.3.4 qualification](docs/maintenance-qualification.md) and
[0.3.5 inspection qualification](docs/delivery-audit.md#qualification-and-rollout)
for source provenance, scope and limits. Qualification does not deploy or automatically
upgrade an application. The stable publication workflow derives the package
version from its release tag. The published stable package is `0.3.5`; the
maintenance source POM remains `0.3.5-SNAPSHOT` for development. Select the stable
package explicitly when upgrading an integration.

For remote acceptance, use the manually triggered **Publish Maven SNAPSHOT**
workflow after the candidate's CI passes. See [maintenance snapshot publication](docs/maintenance-qualification.md#snapshot-publication)
for publication, consumption and provenance requirements. Snapshots are test
artifacts; production integrations keep an explicitly selected stable version.

## Configurable inspection in 0.3.5

The stable `0.3.5` package retains the 0.3.4 Kafka replay and failure-ownership safeguards.
It adds opt-in recording of independent outbound requests on exact `direct:`
route ids and non-replayable inspection of selected non-Kafka inputs. These
optional selectors do not change the platform
certified baseline or automatically enable inspection. See
[delivery audit configuration](docs/delivery-audit.md) and the
[non-Kafka input inspection](docs/input-audit.md) reference and the
[maintenance configuration reference](docs/configuration.md).

## Installation

Stable release coordinates (verify the tag publication completed before upgrading):

```xml
<dependency>
  <groupId>no.basis.integrationplatform</groupId>
  <artifactId>integration-platform-adapter-camel-quarkus</artifactId>
  <version>0.3.5</version>
</dependency>
```

## V1 Semantics

The unreleased 0.3.6 candidate converts the owned `x-bip-correlation-id` from
Kafka's byte-array header representation to text before downstream processing.
This preserves the same ID through HTTP forwarding and journal recording. It
does not convert unrelated binary headers or introduce support for arbitrary
header types. Envelope decoding uses UTF-8 independently of the business payload
charset. See [maintenance qualification](docs/maintenance-qualification.md).

The adapter aims for:

```text
at-least-once processing
+ durable operational status/audit
+ stable idempotency keys
+ controlled retry/replay
```

It does not promise global exactly-once effects across Kafka, Postgres, and
external systems. With a durable `StepOutcomeStore`, the adapter can do better
than plain at-least-once for platform-managed replays: steps already recorded as
delivered can be skipped before repeating the endpoint send. That protects
against duplicate side effects the adapter has already observed and stored.

This is still not the same as transactional exactly-once with every external
system. A crash after an external side effect but before the step outcome is
stored can still lead to a repeated call on replay. External side effects
therefore still need domain idempotency, for example a unique database key,
deterministic file name, stable Kafka event id, or a target API that honors
`Idempotency-Key`.
A receiver can also apply an effect and lose its response; automatic delivery
retries can then repeat that effect before any operator replay. The receiver must
enforce the stable key or equivalent domain uniqueness. Establish the receiver
outcome before requesting replay when acknowledgement is missing.

## Message Status Semantics

V1 records terminal route outcomes only. Normal Camel retry/redelivery failures
are not written as separate `FAILED` attempts; if a retry later succeeds, the
adapter records one `COMPLETED` outcome with Camel redelivery diagnostics.

Platform `attempt` is reserved for manual replay/resend attempts. Camel automatic
redeliveries do not increment `attempt`; they are captured with:

- `auto_redelivered`
- `auto_redelivery_count`
- `auto_redelivery_max`

- `COMPLETED`: route processing finished and the outcome was safely recorded.
- `DLQ`: retries were exhausted and the message was parked for operator/platform action.
- `FAILED`: a failed exchange that was not successfully parked as DLQ.
- `RECEIVED`: reserved for platform capture, not emitted by the Camel adapter terminal-status flow in V1.

## Runtime Behavior

Adding this dependency to a Camel Quarkus integration installs the platform
behavior through CDI:

- `MessageStoreRoutePolicyFactory` applies `MessageStoreRoutePolicy` only to Kafka input routes.
  Internal `direct:` and HTTP request exchanges are not independent replayable messages.
  Selected `direct:` requests may be recorded for inspection through `basis.delivery-audit.routes`; their payloads remain non-replayable.
- `PlatformRouteConfiguration` installs shared retry and DLQ behavior.
- `StepTrackingInterceptStrategy` observes Camel endpoint sends.
- `MessageStoreSinkProducer` selects the message-store sink.
- `StepOutcomeStore` selects the durable step/idempotency store.

Step tracking applies to endpoint sends that may represent external side effects.
Internal routing, observability, validation, and transformation endpoints such as
`direct:`, `log:`, `micrometer:`, `validator:`, `xslt:`, and `xslt-saxon:` are
excluded. They continue to run during replay and are not written to the durable
step ledger.

Kafka consumers must use manual commit. The adapter provides these defaults in
its own `application.properties`:

```properties
camel.component.kafka.auto-commit-enable=false
camel.component.kafka.allow-manual-commit=true
camel.component.kafka.break-on-first-error=true
camel.component.kafka.poll-on-error=RECONNECT
```

The offset is committed only after `MessageStoreSink.record(...)` succeeds and
processing completed or DLQ publication was confirmed. An unparked `FAILED`
outcome, failed journal write or failed commit leaves the exchange failed.
`break-on-first-error` prevents a later record in the batch from advancing past
that unresolved input. Applications must not override these safety defaults.
This can intentionally block processing during a durability outage; it is not a
promise that every failure always progresses.

A failure while polling, before any record reaches a route, reconnects the
consumer and polls the same position again. Camel's default for poll failures
would route the exception to the error handler without a record, move past the
offset and, after an authorization error, stop polling for good while the
process stays up. A missing read permission, for example while a new topic's
permission is still being applied, now delays consumption instead of ending it.
An authentication failure still stops the consumer.

The current Message Store summary follows the highest recorded attempt. A late
write from an older attempt still records its attempt history but cannot replace
the newer summary's status, payload, error or timestamps. Repeated writes for the
same attempt may update that attempt and its current summary; this is not a
within-attempt event-ordering guarantee.

## Retry and DLQ

The global route configuration applies to all routes unless a route opts into a
named variant:

- default: 3 redeliveries, 1 second initial delay, exponential backoff.
- `ip-slow-backend`: 6 redeliveries, 5 second initial delay, exponential backoff.
- `ip-no-retry`: no redelivery before DLQ.

All variants use `basis.message-store.dlq-uri`, defaulting to
`kafka:bip.message.dlq`. When the dead letter channel parks a message, the
adapter marks the exchange only **after the DLQ send succeeds**, so the terminal
status becomes `DLQ`. The internal parking route bypasses business-step tracking;
parking must never be skipped because of a previously delivered business step.
An internal request without a managed Kafka input propagates its failure back
to its caller rather than parking transformed receiver bytes independently.

A competing application `onException` can bypass this recovery handler. Do not
install a catch-all recovery policy alongside the platform handler. Nested
request routes should use `ip-no-retry` when the input route owns retry, to avoid
multiplying attempts. Splits that belong to the same input must share the unit
of work so failure reaches the input's terminal outcome.

Processing time still needs an application-specific budget: bounded receiver
timeouts and retries, number of transactions per input, poll batch size and
`max.poll.interval.ms` must fit together. This patch does not classify every
HTTP 500 as permanent or automatically resize application memory.

The DLQ record follows contract section 8:

- the body is the **original consumed payload**, restored from the payload
  captured at route start — not a mid-transformation body. When the payload
  could not be captured (`metadata_only` mode or an uncapturable body type),
  the current body is parked as-is.
- all `x-bip-*` envelope headers are preserved, plus two diagnostics headers
  set at parking time: `x-bip-error` (the failure message) and `x-bip-dlq-at`
  (RFC 3339 timestamp).
- the attempt is recorded with status `DLQ`, and the Kafka offset is committed
  only after the outcome record (and DLQ publish) succeeded.

## Dashboard Read Model

The direct Postgres sink writes two dashboard-facing tables:

- `bip_message_log`: one current row per message, intended for list/search views.
- `bip_message_attempt`: one row per attempt, intended for message detail/history views.

The list view should normally read from `bip_message_log`:

```sql
select message_id,
       correlation_id,
       integration,
       status,
       last_attempt,
       occurred_at,
       source_system,
       target_system,
       content_type,
       payload_storage,
       payload_size_bytes,
       payload_expires_at,
       payload_deleted_at,
       replayable,
       replay_disabled_reason,
       auto_redelivered,
       auto_redelivery_count,
       auto_redelivery_max,
       error
from bip_message_log
where integration = ?
order by occurred_at desc
limit ?;
```

The detail view should read current message state from `bip_message_log` and
attempt history from `bip_message_attempt`:

```sql
select attempt,
       route_id,
       status,
       occurred_at,
       auto_redelivered,
       auto_redelivery_count,
       auto_redelivery_max,
       steps_json,
       error
from bip_message_attempt
where message_id = ?
order by attempt;
```

`steps_json` is deliberately JSONB for now. If dashboard usage needs richer
step filtering or aggregation, it can move to a dedicated step table without
changing business routes.

Manual replay audit is stored in `bip_message_replay_audit`:

```sql
select replay_id,
       replay_attempt,
       requested_by,
       reason,
       redo_steps,
       target_topic,
       status,
       requested_at,
       published_at,
       error
from bip_message_replay_audit
where message_id = ?
order by requested_at desc;
```

## Payload Storage

V1 stores replay-eligible input payloads inline in Postgres by default:

- `payload_storage = 'postgres_inline'`
- `payload_size_bytes` records the original incoming body size when the adapter
  can determine it safely.
- `payload_bytes` stores the original incoming body for `String` and `byte[]`
  messages when it is within the configured cap.
- `payload_expires_at` is set from the payload retention policy.
- `replayable` is true when the platform currently has the payload and policy
  allows manual replay.

The adapter deliberately does not consume stream-like or unknown body types just
to capture payload bytes. Those messages fall back to:

- `payload_storage = 'metadata_only'`
- `replayable = false`
- `replay_disabled_reason` explaining why replay is unavailable.

Payload retention is separate from metadata retention. A future cleanup job can
clear `payload_bytes`, set `payload_deleted_at`, flip `replayable` to false, and
keep the dashboard/audit metadata in place.

Payload storage captures the incoming body at route start. It currently stores
`String`, `byte[]`, and `null` bodies. Stream-like or unknown body types are not
consumed by the adapter just to make replay possible.

## Manual Replay

V1 includes `MessageReplayService`, backed by Postgres payload storage and Kafka
republish. It loads a replayable row from `bip_message_log`, validates that
payload bytes are still available, writes an `bip_message_replay_audit` row, and
publishes the stored payload back to the integration input topic.

The adapter also exposes the dashboard/agent runtime admin contract:

```http
POST /admin/messages/{messageId}/resend
```

The endpoint calls `MessageReplayService`, reads `x-dashboard-user` and
`x-dashboard-reason` into the replay audit, and returns a JSON result containing
`status`, `messageId`, `replayId`, `integration`, `topic`, and `attempt`.
Optional JSON request fields `requestedBy`, `reason`, and `redoSteps` are also
accepted for future callers that prefer a body over headers.

Callers can use `MessageReplayRequest` to include operator context:

- `messageId`
- `requestedBy`
- `reason`
- `redoSteps`

Replay publishes with:

- the same `x-bip-message-id`
- the same correlation/content/source/target envelope headers
- `x-bip-replay=true`
- `x-bip-attempt=<last_attempt + 1>`
- `x-bip-redo-steps=<JSON array of step ids>` when redo steps are supplied,
  e.g. `["sftp-upload","webhook-notify"]` — the same JSON array stored in the
  `redo_steps` audit column (contract DECISION D1)

By default, the input topic is derived from
`basis.message-store.replay.input-topic-pattern`, default `bip.%s.in`, where `%s`
is the stored integration name. Test/demo integrations can override this to a
fixed topic.

### Redo behavior

Replay does not bypass idempotency by default. If
`basis.message-store.step-outcome-store` is `postgres`, already delivered steps
with the same `<message-id>:<step-id>` idempotency key are skipped and recorded
as `SKIPPED` on the replay attempt.

`x-bip-redo-steps` is the explicit operator override (contract section 9): the
step tracker parses the header as a JSON array of step ids and re-executes
exactly the listed steps even when the durable ledger already has them
`DELIVERED`. A step matches when the list contains its step id or its full
idempotency key. A malformed header value is logged as a warning and treated as
empty — it never fails message processing. Redo intentionally repeats an
external side effect, so external systems still need domain idempotency for it.

## Step Tracking

The adapter observes Camel endpoint sends with a Camel `InterceptStrategy`. For
stable step names, use normal Camel ids on important external sends:

```yaml
- to:
    id: deliver-vendor-order
    uri: "sftp:vendor/orders?fileName=order-${header.x-bip-correlation-id}.xml"
```

If no id is present, the adapter derives a fallback key from route and endpoint
metadata. Fallback keys are acceptable for audit, but explicit ids are better
for dashboard display and future partial replay.

Internal Camel endpoints such as `direct:`, `log:`, `bean:`, `mock:`, `seda:`,
`timer:`, and `micrometer:` are ignored. External sends get:

- `x-bip-idempotency-key = <message-id>:<step-id>`
- `Idempotency-Key = <message-id>:<step-id>`

The adapter records `DELIVERED`, `FAILED`, or `SKIPPED` step outcomes in the
message attempt. `SKIPPED` means the configured `StepOutcomeStore` says the same
idempotency key was already delivered, so the endpoint send was not repeated.

Each recorded step keeps its endpoint URI with credentials masked the way Camel
masks them in logs: a password in the user info and options such as `password`,
`passphrase`, `secretKey` or `accessToken` are stored as `xxxxxx`. Step ids are
derived from the exact URI, so masking does not change them. Failed and skipped
step entries use the same masked endpoint, as does the warning emitted when a
send has no stable Camel node id. This masks the endpoint field, not arbitrary
exception messages or application logs. Existing rows, logs and backups are not
rewritten; review their access and retention separately.

## Key-Value Store

The combined local `0.3.6-SNAPSHOT` candidate adds
[`KeyValueStore`](docs/key-value-store.md) for small integration-scoped JSON
cursors and markers in the platform-provisioned `bip_key_value` table of draft
contract `1.2.0-rc2`. It keeps the full maintenance baseline and adds no leases
or Trusted Message Store service. It is not a published or certified pin.
Calls have their own transactions; they are not atomic with delivery or polling.
The feature is lazy and has no effect when unused. Calls fail explicitly if the
table is unavailable; no schema or metadata is created at startup.

## Sink and Store Selection

The message outcome sink is selected with `basis.message-store.sink`:

- `log`: logs message outcomes; useful for local smoke tests.
- `postgres`: writes `bip_message_log` and `bip_message_attempt` directly.
- `kafka`: emits terminal status events to `basis.message-store.status-topic`,
  default `bip.message.status`.

`postgres` requires a configured Quarkus JDBC datasource and the message-store
schema to exist. `kafka` requires `kafka.bootstrap.servers` and is only complete
when a central message-store/status service consumes `bip.message.status` and
writes the canonical Postgres read model. Without that service, the Kafka sink
only publishes status events; nothing records them to Postgres.

For V1 manual replay with this adapter, use the `postgres` sink. The `kafka`
sink is status-event output only; it does not store payload bytes by itself.

The step outcome store is selected with `basis.message-store.step-outcome-store`:

- `noop`: default; idempotency keys are injected and step results are collected,
  but previously delivered steps are not skipped across attempts or restarts.
- `postgres`: uses `bip_step_outcome` for durable delivered-step checks.

Schema resources are shipped in the adapter JAR:

- `db/message-store/bip_message_store.sql`
- `db/message-store/bip_step_outcome.sql`
- `db/message-store/bip_contract.sql`

They are test/local schema resources, not an integration-owned migration
strategy. In production, whoever operates the database provisions the schema
outside the integration pods.

### Contract version startup gate

Per contract section 10, when `basis.message-store.sink=postgres` and a JDBC
datasource is available, the adapter reads
`select max(contract_major) from bip_contract` at startup:

- table missing (or empty): a loud warning is logged and startup continues,
  assuming contract major 1 (v1.0 rollout rule).
- `max(contract_major) != 1`: startup fails with an error naming the found and
  expected major, instead of silently writing a mismatched schema shape.

## Configuration

Adapter defaults:

```properties
basis.message-store.sink=log
basis.message-store.status-topic=bip.message.status
basis.message-store.step-outcome-store=noop
camel.component.kafka.auto-commit-enable=false
camel.component.kafka.allow-manual-commit=true
basis.message-store.dlq-uri=kafka:bip.message.dlq
basis.message-store.payload.mode=postgres-inline
basis.message-store.payload.max-bytes=1048576
basis.message-store.payload.retention-days=30
basis.message-store.replay.enabled=true
basis.message-store.replay.input-topic-pattern=bip.%s.in
```

Configuration reference:

| Property | Values | Default | Notes |
| --- | --- | --- | --- |
| `basis.delivery-audit.routes` | comma-separated exact route ids | empty | Select independent `direct:` delivery requests for non-replayable audit. |
| `basis.input-audit.routes` | comma-separated exact route ids | empty | Select non-Kafka input inspection without changing source recovery. |
| `basis.message-store.key-value.sweep-interval-seconds` | integer seconds, `1..3600` | `3600` | Once the candidate Key-Value Store is used, sweep this integration’s expired keys in bounded batches. |
| `basis.message-store.sink` | `log`, `postgres`, `kafka` | `log` | Selects where terminal message outcomes go. |
| `basis.message-store.step-outcome-store` | `noop`, `postgres` | `noop` | Selects whether delivered-step checks are durable. |
| `basis.message-store.payload.mode` | `postgres-inline`, `metadata-only` | `postgres-inline` | Selects whether safely capturable payloads are stored inline for replay. |
| `basis.message-store.status-topic` | Kafka topic name | `bip.message.status` | Used only when `basis.message-store.sink=kafka`. |
| `basis.message-store.dlq-uri` | Camel endpoint URI | `kafka:bip.message.dlq` | Target used by the shared dead letter channel. |
| `basis.message-store.payload.max-bytes` | integer bytes, `>= 0` | `1048576` | Payloads above this cap become `metadata_only`. |
| `basis.message-store.payload.retention-days` | integer days, `>= 0` | `30` | Used to set `payload_expires_at`. Cleanup is a separate future job. |
| `basis.message-store.replay.enabled` | `true`, `false` | `true` | If false, payloads are metadata-only and replay is disabled by policy. |
| `basis.message-store.replay.input-topic-pattern` | Java format string with one `%s`, or fixed topic | `bip.%s.in` | Used by `MessageReplayService` to choose the replay target topic. |
| `camel.component.kafka.auto-commit-enable` | `true`, `false` | `false` | Must stay false for adapter-managed commit. |
| `camel.component.kafka.allow-manual-commit` | `true`, `false` | `true` | Must stay true for adapter-managed commit. |

Direct Postgres V1 mode for platform-managed integrations:

The adapter JAR defaults to `log` and `noop` so adding the dependency does not
force every local/test integration to have Postgres available. Customer-facing
Camel Quarkus integrations should get the production defaults from the platform
template, not from the adapter library itself.

```properties
basis.message-store.sink=postgres
basis.message-store.step-outcome-store=postgres
basis.message-store.payload.mode=postgres-inline
basis.message-store.replay.enabled=true
basis.message-store.replay.input-topic-pattern=bip.%s.in
```

These settings belong in the platform's Camel Quarkus integration template
for V1 unless an integration has a deliberate reason to opt out.

Tests and local smoke routes can override `basis.message-store.dlq-uri` to a
non-Kafka endpoint, for example `direct:test-dlq`, when no Kafka broker is
available.

## Status

- Direct Postgres mode is the v1 production path. Platform templates set the
  production defaults and pin the adapter version; generated integrations do
  not carry their own resend or DLQ plumbing.
- The adapter is certified against message-store contract v1.0.0-rc2: the
  cross-runtime certification suite runs this adapter and the .NET adapter
  (`Basis.IntegrationPlatform.MessageStore` on NuGet.org) against real Kafka
  and Postgres and diffs the resulting database state across runtimes.
- The message-store schema is provisioned platform-side from the canonical
  contract DDL; the SQL files shipped in this JAR are local/test resources
  only.
- The Kafka status-event sink exists for the target hybrid model but is not
  the v1 production path: nothing records `bip.message.status` events to
  Postgres until a central message-store service consumes them.

## V2 Direction

V2 is an evolution of the contract, not a rewrite of business routes.

Likely V2 work:

- A per-cluster platform message-store service that captures messages at the
  Kafka boundary (real `RECEIVED`/in-flight visibility) and becomes the single
  journal writer.
- Move from adapter-direct Postgres persistence to status/lifecycle events
  once that service owns persistence. The existing `MessageStoreSink`
  boundary is the switch point — migration is a per-integration sink flip.
- Split `steps_json` into dedicated relational tables only if dashboard query
  needs justify that change.
- Consider external payload storage for large/sensitive payloads instead of
  inline Postgres bytes.

## License and support

Apache-2.0. The adapter is developed as part of the Basis integration
platform; production use is supported for platform customers through their
platform agreement. Issues and pull requests from other users are welcome and
handled best-effort.

## Authenticated platform broker

Version 0.3.3 adds one operator-owned security configuration for Camel Kafka
consumption/DLQ and the direct Kafka status/replay producers. It retains the
0.3 runtime model; the separate 0.4 development line is not required.

Set `basis.message-store.kafka.security-protocol=SASL_SSL`,
`basis.message-store.kafka.sasl-mechanism=SCRAM-SHA-512` and the secret
`basis.message-store.kafka.sasl-jaas-config`. The platform supplies the broker
CA through `basis.message-store.kafka.ssl-truststore-location`; select `PEM`
with `basis.message-store.kafka.ssl-truststore-type` for a PEM certificate.
Hostname verification stays enabled. Missing SASL credentials fail startup.
Default plaintext behavior remains for environments that have not opted in.

The adapter's PostgreSQL datasource belongs only to the platform Message Store.
Application state must use its separately approved database and a named Quarkus
datasource with its own credentials, migrations, limits and recovery policy.
Never create business tables in the adapter's default database.

## Maintenance replay validation

Before writing replay audit or publishing, the service requires the current
message's latest stored attempt to have matching integration and payload bytes,
and its route ID to identify a Kafka input in the running integration. That
input must consume exactly the configured replay topic; regex subscriptions,
other topics, removed routes and internal request routes are rejected. This
also blocks unsafe rows created before the upgrade, without deleting history.
The integration's `app.name` must match its stored integration identity.

Existing dashboard rows may still advertise replayability from old capture
metadata; the server-side check is authoritative and returns a refusal. The
patch does not rewrite historical rows or remove records already published to
Kafka. Safe input replay still retains message identity, increments the manual
attempt and preserves the existing redo-step contract. Request acceptance is
not proof of delivery or of successful offset commit.

The [application database continuity fixture](integration-tests/application-data-smoke/README.md)
checks a named business datasource alongside a legacy Message Store with no KV
table. Its baseline and candidate results must be recorded separately; upgrading
the adapter does not move application state.

## Optional activity metrics candidate

The unreleased candidate adds explicitly selected Kafka admission and
broker-confirmed publication counters through an existing Micrometer registry.
They measure activity, not unique transactions or downstream completion. Defaults
remain disabled; missing or incomplete coverage is unknown. See
[activity metrics](docs/activity-metrics.md) and
[configuration](docs/configuration.md#optional-kafka-activity-metrics).

## How this project is developed

Most code and documentation here is written with AI coding agents, directed
and reviewed by Basis maintainers. Every change goes through a pull request,
human review and CI before it is merged. [`AGENTS.md`](AGENTS.md) contains the
rules the agents follow; human contributors follow the same rules.

See [CONTRIBUTING.md](CONTRIBUTING.md) for branches and pull requests.

## Release history

This repository starts from a fresh history. Versions up to 0.3.5 were
developed in an earlier private repository, so this repository has no tags or
release pages for them. Their notes are in [CHANGELOG.md](CHANGELOG.md).
