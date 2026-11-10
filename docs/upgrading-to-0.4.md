# Upgrading from 0.3.x to 0.4.0

Status: **for the unreleased 0.4.0.** This page describes the development
branch. It grows with each change until 0.4.0 is released; a change with an
upgrade action adds it here and in the [changelog](../CHANGELOG.md).

0.3.x stays a maintained series, so upgrading is a choice, not a requirement.
Start from 0.3.6; the 0.3.x notes in the changelog apply to earlier versions.

## Before You Upgrade

- **Bring the Message Store to the shipped schema.** With the Postgres sink or
  the Postgres step store, the adapter refuses to start against a store that
  lacks a table or column it writes, and its error lists the statements that
  add them: `bip_message_log` and `bip_message_attempt` with the Postgres sink,
  `bip_message_replay_audit` as well when replay is enabled, and
  `bip_step_outcome` with the Postgres step store. The schema ships in
  `src/main/resources/db/message-store/`; the database operator applies it.
- **Kafka header names on the newer Quarkus line.** This branch builds against
  Quarkus 3.33.4 (Camel 4.18.4), whose camel-kafka exchange headers are named
  `CamelKafka*` instead of `kafka.*`. An integration that moves to this line
  and reads a header by its old literal name, such as `kafka.KEY` in a YAML
  route, must use the new name, such as `CamelKafkaKey`. A Java route that uses
  `KafkaConstants` gets the new names when it is compiled.
- **Keep the required Kafka consumer settings**, in particular
  `camel.component.kafka.break-on-first-error=true`; see the
  [configuration reference](configuration.md).
- **Classify sends.** 0.3.6 tracked every send to a non-internal endpoint as a
  side effect. On this branch the default classifier classifies internal
  schemes as internal (not a step), write-only producers (`kafka`, `jms`,
  `sjms`, `sjms2`, `amqp`, `amqp10`, `file`, `ftp`, `ftps`, `sftp`) as side
  effects, which are tracked, and HTTP sends with a safe method (`GET`,
  `HEAD`, `OPTIONS`, `TRACE`) as repeatable. Every other send is ambiguous: every POST, PUT, PATCH or DELETE, an HTTP send with
  Camel's default method, and sends such as `sql:`, `jdbc:`, `aws2-s3:` or
  `smtp:`. An ambiguous send runs on every redelivery, resend and replay and is
  not protected by the step ledger. The adapter logs a warning for it when the
  first message reaches the send, once per node and scheme, not at startup.
  Give each such send a stable node id and a role with the
  `basis.message-store.step.*-ids` properties; see
  [Steps and idempotency](steps-and-idempotency.md). That 0.4.0 will require a
  role for every such send, set in the application's own code, is decided but
  not implemented yet
  ([#68](https://github.com/basis-no/integration-platform-adapter-camel-quarkus/issues/68)).
- **Remove the delivery-audit and input-audit settings.** 0.3.5 and 0.3.6
  record `direct:` request routes named in `basis.delivery-audit.routes` and
  non-Kafka inputs named in `basis.input-audit.routes`. This version implements
  neither, and the application refuses to start when either setting, or any
  other name under `basis.delivery-audit.` or `basis.input-audit.` (also in
  its environment form), has a value, naming each one and what to use instead; an empty value only logs a
  warning. Remove `basis.delivery-audit.routes`: delivery audit is not
  available in this version yet
  ([#93](https://github.com/basis-no/integration-platform-adapter-camel-quarkus/issues/93)).
  Replace `basis.input-audit.routes` with the
  [HTTP audit-only preview](ingress-and-scaling.md#http-audit-only-preview)
  for an HTTP input, or ingress normalization to Kafka for a file, timer or
  poller input. `boundary-routes` does not replace them. See
  [Settings Removed Since 0.3.x](configuration.md#settings-removed-since-03x).
- **Use `earliest` and a stable `groupId` on every Kafka boundary.** A Kafka
  boundary's consumer resets to `earliest`, and a boundary route whose consumer
  uses any other `auto.offset.reset` refuses to start, naming the route and
  its endpoint. With 0.3.6's `latest`, a record that was not committed on a
  partition without a committed offset could be skipped after a reconnect.
  Remove an `autoOffsetReset=latest` or `none` override from the endpoint, its
  `additionalProperties.auto.offset.reset` and
  `camel.component.kafka.auto-offset-reset`. A consumer group with committed
  offsets continues from them. A new group, a changed `groupId`, or a group
  whose committed offsets expired on the broker now starts at the beginning of
  each partition's retained records instead of at the end; to start one at the
  end, set its offsets with the Kafka tools before its first start. A boundary
  without a `groupId` gets a new group from Camel on every start, so it now
  reprocesses every retained record on each start: give it a stable `groupId`.

## Behaviour That Changed on Purpose

### Stricter `dlq-uri`

`basis.message-store.dlq-uri` must name one fixed `kafka:` endpoint whose
producer waits for the broker and is not transactional, or the application's
own `direct:` route. Startup fails for an expression such as `${header.name}`,
for Kafka with `acks=0` or a transactional producer (`transacted`,
`transactionalId` or `transactional.id`), for `direct:` with
`failIfNoConsumers=false` (for example
`dlq-uri=direct:park?failIfNoConsumers=false`), and for any other endpoint,
such as `file:`. 0.3.x accepted these, and some of them could count a message
as parked although no endpoint had accepted it. Point `dlq-uri` at a Kafka
topic, or at a `direct:` route that the application consumes and that fails
when it cannot park. See
[Terminal Kafka Failure](retry-dlq-replay.md#terminal-kafka-failure).

A Kafka DLQ now gets one record per message also when the parked body is a
collection; 0.3.6 sent one record per element, and none for an empty
collection.

### A stopped exchange that fails is read again

A processor that calls `exchange.setRouteStop(true)` and then fails is recorded
`FAILED`, its offset is not committed, and the record is read again, so its
partition waits. 0.3.x recorded it `COMPLETED` and committed it, so the
failure was lost. Camel skips the dead letter channel for a stopped exchange,
so such a message is not parked either. Do not stop an exchange that is about
to fail; let the failure reach the adapter.

### Which routes are recorded: `boundary-routes`

0.3.6 made a message boundary of every Kafka input route, and recorded other
routes only through the separate delivery-audit and input-audit settings,
which this version refuses (see above). 0.4 selects its message boundaries with
`basis.message-store.boundary-routes`:

- `auto`, the default, selects Kafka consumer routes, as 0.3.6 did. Nothing
  changes for an application that does not set it.
- Explicit route ids, or `all`, select those routes, or every route except the
  adapter's own parking route. A selected route must be a Kafka input, or a
  `platform-http:` route for the [HTTP audit-only
  preview](ingress-and-scaling.md#http-audit-only-preview); any other selected
  route, such as a nested `direct:` route or a timer, refuses to start, and so
  does an ingress normalization route. Kafka boundaries and the HTTP preview can
  run in one application: add the HTTP route id next to `auto`, for example
  `auto,receive-order`, so that a Kafka route added later is still selected.
  `all` starts only when every route is a Kafka input or an HTTP preview
  route.

A selected route that is not a Kafka input is never replayed into the input
topic
([#101](https://github.com/basis-no/integration-platform-adapter-camel-quarkus/issues/101)):
startup refuses every selected route that is neither a Kafka input nor the HTTP
preview, and an HTTP preview route is never replayable. A replay
publishes only when the message's last attempt was recorded by a Kafka route of
this application that consumes the replay topic; a message recorded by a Kafka
route on another topic is refused before any replay audit row is written.

Leave `boundary-routes` at `auto` unless the application needs the HTTP
preview.

### Object-backed inputs are parked as their claim check

Object storage (`tiered` payload mode) is new in 0.4. A Kafka input whose
payload is in object storage is parked in the DLQ as its claim-check document
(`schemaVersion`, `storage`, `ref` such as `s3://bucket/key`, `sizeBytes`,
`sha256`, `contentType` and `expiresAt`), not as its bytes. A tool that reads
the DLQ gets the claim check and must fetch the object to see the payload.

Known limitation
([#99](https://github.com/basis-no/integration-platform-adapter-camel-quarkus/issues/99)):
the DLQ record is usable only while the object exists and its expiry has not
passed. The object's expiry is fixed when it is captured, at capture time plus
`basis.message-store.payload.retention-days` (30 days by default), and a
replay does not extend it. The adapter refuses an expired or missing object,
so a DLQ record resent after that fails processing. The adapter does not relate
this to the DLQ topic's retention. Keep the payload retention, and the bucket's
lifecycle expiry, at least as long as the DLQ topic's retention.

## Not in This Build Yet

The 0.3.6 notes say that 0.4 gives each split part, loop iteration and branch
its own step key, and each integration its own message id. Neither is on this
branch yet; both must land before 0.4.0
([#31](https://github.com/basis-no/integration-platform-adapter-camel-quarkus/issues/31),
[#30](https://github.com/basis-no/integration-platform-adapter-camel-quarkus/issues/30)).
Until then the 0.3.x limitations for splits and shared message ids apply; see
[Steps and idempotency](steps-and-idempotency.md).

Also not on this branch yet:

- **The shared message-id warning.** 0.3.6 warns when a message id was already
  journaled by another integration, which then takes over that row; this
  branch does not warn yet
  ([#100](https://github.com/basis-no/integration-platform-adapter-camel-quarkus/issues/100)).
- **Delivery audit** (`basis.delivery-audit.*`)
  ([#93](https://github.com/basis-no/integration-platform-adapter-camel-quarkus/issues/93));
  startup refuses the setting, see [Before You Upgrade](#before-you-upgrade).

## Other Upgrade Actions from 0.3.6

The changelog's Unreleased section has the details; in short:

- **The failure handling follows the input type.** A Kafka boundary parks a
  failed message in the DLQ and commits after it; an HTTP audit-only boundary
  records `FAILED`, answers its caller with the error and is never replayable,
  whatever `basis.message-store.failure-mode` and
  `basis.message-store.replay.enabled` say; a normalized file, poll or queue
  input hands the failure back to its source. An HTTP preview no longer needs
  `failure-mode=record-failed` or `replay.enabled=false`. `record-failed` is
  still accepted, and still refused in an application with a Kafka boundary.
  See [Failure and Replay per Input Type](retry-dlq-replay.md#failure-and-replay-per-input-type).
- **A JMS queue input needs `CLIENT_ACKNOWLEDGE`.** Set
  `acknowledgementModeName=CLIENT_ACKNOWLEDGE` on a `jms:queue:` normalization
  input; with camel-jms's default `AUTO_ACKNOWLEDGE`, a message whose route
  failed is not delivered again
  ([#123](https://github.com/basis-no/integration-platform-adapter-camel-quarkus/issues/123)).
  See [JMS Queues](ingress-and-scaling.md#jms-queues).

- **Shorter `ip-slow-backend` waits.** The `ip-slow-backend` route
  configuration caps each redelivery delay at 20 seconds: 5, 10, 20, 20, 20
  and 20 seconds, 95 seconds in all, instead of 0.3.6's 195 seconds (Camel's
  default 60-second cap). `max.poll.interval.ms` covers every record of one
  poll, so for a route on `ip-slow-backend` set `maxPollRecords=1` on its Kafka
  input or raise `maxPollIntervalMs`. See
  [Automatic Retry](retry-dlq-replay.md#automatic-retry).
- **Spaced re-reads of a record whose outcome is not durable.** 0.3.6 read
  such a record again at once, several times a second. Now the first read
  again is at once, then each further read of the same record waits 1 s,
  doubling, at most 30 s and at most half of `max.poll.interval.ms` less the
  time its poll has already taken; a committed outcome resets the wait. The
  consumer's other partitions wait with it. The outcome is still not committed.
  Without `break-on-first-error=true` there is no wait.
- **Requests on exchanges of their own fail back to their caller**, as in
  0.3.6: only a managed Kafka input is parked. Unlike 0.3.6, such a failure no
  longer logs the parking ERROR. A route that sends requests on exchanges of its
  own must handle their failures, or let them fail the input; a timer or poller
  route sees its failure instead of a DLQ record.
- **Activity counters on by default.** An application with a Kafka boundary
  that did not set `basis.traffic.signals`, or set it empty, now exports the
  admission counter; on 0.3.6 an empty value turned counting off. Set
  `basis.traffic.signals=none` to keep 0.3.6's default. See
  [Kafka activity metrics](activity-metrics.md).
- **Sub-route sends recorded once.** A message that 0.3.x parked after a failed
  send in a `direct:` sub-route keeps its `DELIVERED` step; resend it with the
  step in `redoSteps`. See
  [Sends in a sub-route](steps-and-idempotency.md#sends-in-a-sub-route).
- **Redo needs a recording step store.** A replay whose `redoSteps` names a
  step is refused with `409 Conflict` when the step outcome store is `noop`,
  the default; 0.3.x had no such check. Resend without `redoSteps`, or use the
  `postgres` step store. An application's own `StepOutcomeStore` that records
  nothing should override `recordsDeliveredSteps()` to return `false`;
  otherwise redo is still accepted. See
  [Retry, DLQ, and replay](retry-dlq-replay.md).
- **Credential headers are withheld on every HTTP send.** A consumed record's
  credential headers and the consumer's `kafka.*` and `CamelKafka*` headers are
  left out of every send to an `http`, `https`, `netty-http` or `vertx-http`
  endpoint, also through `enrich`, `wireTap`, `recipientList`, `routingSlip`
  and a `ProducerTemplate`; 0.3.6 withheld them only on a tracked `to` or `toD`
  to `http:` or `https:`. A receiver that got a credential forwarded from the
  record may now answer 401. To forward one deliberately, set the header in the
  route as text, for example
  `setHeader("Authorization", header("Authorization").convertToString())`. A
  receiver that read `kafka.*` request headers must get the values from
  headers the route sets itself.
- **Hand-built test contexts.** A test that adds `PlatformRouteConfiguration`
  to a hand-built `CamelContext` must also add `PlatformDeadLetterRoute`;
  Quarkus applications get both.
- **Health.** Add Camel's health bridge and a 3–5 second readiness probe
  timeout, as for 0.3.6; see
  [Health and Readiness](getting-started.md#health-and-readiness).
