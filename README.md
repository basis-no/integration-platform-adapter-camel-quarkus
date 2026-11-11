# Basis Camel Quarkus Adapter

Shared Camel Quarkus adapter for message-store auditing, Kafka recovery and
controlled replay.

The adapter uses message-store schema version 1. Its identifiers use a `bip`
prefix: Kafka topics `bip.*` (`bip.<integration>.in`, `bip.message.dlq`,
`bip.message.status`), Postgres tables `bip_*` (`bip_message_log`,
`bip_message_attempt`, `bip_message_replay_audit`, `bip_step_outcome`,
`bip_contract`), and envelope headers `x-bip-*`. Environment variables
(`BASIS_MESSAGE_STORE_*`) and config keys (`basis.message-store.*`) keep their
prefixes; only values that name topics, tables, or headers use the `bip` names.

The adapter keeps message-store mechanics out of business routes where possible:

- standard `x-bip-*` envelope headers
- message attempt/status collection
- step tracking and idempotency-key injection around endpoint sends
- retry/DLQ route configuration defaults
- Kafka manual offset commit after the message outcome is safely recorded
- a small `MessageStoreSink` boundary so persistence can be direct Postgres now
  or lifecycle events for a separate consumer later

This library is a Camel/Quarkus runtime adapter. It is self-contained: the
tables, topics and headers it reads and writes are documented in this README,
and the schema it expects ships in `src/main/resources/db/message-store/`.

See the [changelog](CHANGELOG.md) for changes, upgrade actions and
limitations. [Release tooling](docs/release-packaging.md) builds unsigned candidates
without production credentials. Final releases are signed with the offline
signer and uploaded by the release manager from reviewed tooling; CI holds no
publishing credentials. A tag never rebuilds or uploads a release. Final notes
need an approved date and exact artifact provenance; source preparation does not
authorize publication. See [Releasing](docs/releasing.md).

## 0.3 maintenance line

The 0.3 line continues as a maintained series, with its current scope (one
input topic per integration); further 0.3.x patch releases may follow, also
after 0.4.0. `0.3.6` is the current maintenance release. It retains the 0.3.4 replay and
failure-ownership safeguards and the 0.3.5 inspection options. See
[0.3.4 qualification](docs/maintenance-qualification.md) and
[0.3.5 inspection qualification](docs/delivery-audit.md#qualification-and-rollout)
for source provenance, scope and limits, and the [changelog](CHANGELOG.md) for
what 0.3.6 changes. Qualification does not deploy or automatically upgrade an
application.

A release is built as an unsigned candidate in CI, without credentials, from the
reviewed source commit; the release tooling sets the release version, while the
source POM stays at `X.Y.Z-SNAPSHOT`. The release manager signs that candidate
offline and uploads it to Maven Central. Select the stable package explicitly
when upgrading an integration.

For acceptance, use a locally built `X.Y.Z-SNAPSHOT` or a snapshot of a reviewed
commit published from the release manager's workstation. See
[Releasing](docs/releasing.md#acceptance-with-locally-built-artifacts) and
[published snapshots](docs/releasing.md#published-snapshots).
Snapshots are test artifacts; production integrations keep an explicitly
selected stable version.

## Configurable inspection in 0.3.5

The stable `0.3.5` package retains the 0.3.4 Kafka replay and failure-ownership safeguards.
It adds opt-in recording of independent outbound requests on exact `direct:`
route ids and non-replayable inspection of selected non-Kafka inputs. These
optional selectors do not change the default
behavior or automatically enable inspection. See
[delivery audit configuration](docs/delivery-audit.md) and the
[non-Kafka input inspection](docs/input-audit.md) reference and the
[maintenance configuration reference](docs/configuration.md).

## Installation

Stable release coordinates (verify the tag publication completed before upgrading):

```xml
<dependency>
  <groupId>no.basis.integrationplatform</groupId>
  <artifactId>integration-platform-adapter-camel-quarkus</artifactId>
  <version>0.3.6</version>
</dependency>
```

## Processing Semantics

From 0.3.6 the adapter converts the owned `x-bip-correlation-id` from
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
than plain at-least-once for adapter-managed replays: steps already recorded as
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

The adapter records terminal route outcomes only. Normal Camel retry/redelivery failures
are not written as separate `FAILED` attempts; if a retry later succeeds, the
adapter records one `COMPLETED` outcome with Camel redelivery diagnostics.

The message `attempt` is reserved for manual replay/resend attempts. Camel automatic
redeliveries do not increment `attempt`; they are captured with:

- `auto_redelivered`
- `auto_redelivery_count`
- `auto_redelivery_max`

The count is Camel's redeliveries, not the requests a receiver saw. The HTTP
client inside Camel's `http` component (Apache HttpClient 5.5 and its default
retry strategy) also retries by itself, within one Camel attempt: it repeats a
request answered with 503 or 429 once, POST included. Before that retry it waits
for the receiver's `Retry-After`, given in seconds or as a date, with no upper
limit; without `Retry-After` it waits one second. The wait runs on the Kafka
consumer thread, so it counts against `max.poll.interval.ms` (300 seconds by
default) like the rest of the processing-time budget described under
[Retry and DLQ](#retry-and-dlq). If processing takes longer, Kafka removes the
consumer from its group and the uncommitted record is delivered again.

Two 503 answers and then success can therefore record one redelivery for three
requests; with the default policy, a receiver that keeps answering 503 can get
eight requests before the message is parked. Set
`camel.component.http.automatic-retries-disabled=true` to leave every retry to
the adapter's retry policy. Set it for receivers that send a long `Retry-After`,
and for routes on the `ip-slow-backend` configuration, whose own redeliveries
already use about a third of the poll interval.

- `COMPLETED`: route processing finished and the outcome was safely recorded.
- `DLQ`: retries were exhausted and the message was parked for operator action.
- `FAILED`: a failed exchange that was not successfully parked as DLQ.
- `RECEIVED`: reserved for a future capture layer, not emitted by the Camel adapter terminal-status flow.

## Runtime Behavior

Adding this dependency to a Camel Quarkus integration installs the message-store
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
camel.component.kafka.auto-offset-reset=earliest
```

From 0.3.7 the adapter ships `auto-offset-reset=earliest`. A record whose
outcome was not durable is not committed, and Camel's reconnect resumes from
the committed offset. On a partition where the consumer group has none yet, it
resumes from `auto.offset.reset`: with Camel's default `latest` that is the
end of the partition, so the record was never read again and a later record's
commit moved past it. A managed Kafka input whose consumer uses another value
(on the component, the endpoint's `autoOffsetReset` or
`additionalProperties.auto.offset.reset`) still starts, with a warning at
startup that names the route, its endpoint without options and the value. So
does a managed Kafka input without a `groupId`: Camel then gives its consumer a
new group on every start, which has no committed offsets, so with `earliest` it
reads every retained record of its partitions again on each start. Give every
Kafka input a stable `groupId`, on the endpoint or as
`camel.component.kafka.group-id`; Camel's consumer does not use
`additionalProperties.group.id`, it replaces it.

The offset is committed only after `MessageStoreSink.record(...)` succeeds and
processing completed or DLQ publication was confirmed. An unparked `FAILED`
outcome, failed journal write or failed commit leaves the exchange failed. A
step outcome that cannot be checked or recorded fails the send's exchange after
Camel's retries; the input is then recorded as `FAILED` and neither parked nor
committed, also when the route catches the failure or it happens in a split
part. A separate request, such as one sent with a `ProducerTemplate`, fails in
the same case but does not hold back the input that sent it: an input that
passes the request's failure on is retried, parked and committed like any
failed input, and a replay repeats the request's send. Work that ends after the
input completed, such as a `wireTap`, cannot hold the input back either.
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

- default: 3 redeliveries after 1, 2 and 4 seconds, 7 seconds of waiting in
  all.
- `ip-slow-backend`: 6 redeliveries after 5, 10, 20, 20, 20 and 20 seconds, 95
  seconds in all. From 0.3.7 each delay is capped at 20 seconds; before, the
  delays doubled up to Camel's default cap of 60 seconds and waited 195
  seconds.
- `ip-no-retry`: no redelivery before DLQ.

On a Kafka input the waits run on the consumer thread, between attempts that
take their own time. `max.poll.interval.ms` (300 seconds by default) bounds the
time between two polls, and the consumer processes every record of a poll
before it polls again (up to 500 records, Camel's default `maxPollRecords`), so
the budget covers all records of one poll, not one record. If they take longer,
Kafka removes the consumer from its group and the uncommitted records are
delivered again, so a record can run, and be parked, more than once. With
`ip-slow-backend`, one record that exhausts its retries waits 95 seconds, so
three such records in one poll already take most of the interval. For a route
on `ip-slow-backend`, set `maxPollRecords=1` on the Kafka input, or raise
`maxPollIntervalMs` on it to cover the records of a poll. With one record per
poll, about 200 seconds remain for its seven attempts: bound each receiver call
well inside that with `responseTimeout` and `connectTimeout` (see the timeout
table below for the forms Camel applies), and turn the HTTP client's own retry
off with `camel.component.http.automatic-retries-disabled=true`, since it can
double the requests in each attempt.

All variants use `basis.message-store.dlq-uri`, defaulting to
`kafka:bip.message.dlq`. When the dead letter channel parks a message, the
adapter marks the exchange only **after the DLQ send succeeds**, so the terminal
status becomes `DLQ`. A send that returns without an error counts as success, so
`dlq-uri` must be a fixed URI of a durable endpoint (see the configuration
reference below). The internal parking route bypasses business-step tracking;
parking must never be skipped because of a previously delivered business step.
An internal request without a managed Kafka input propagates its failure back
to its caller rather than parking transformed receiver bytes independently.

A competing application `onException` can bypass this recovery handler. Do not
install a catch-all recovery policy alongside the adapter's handler. Nested
request routes should use `ip-no-retry` when the input route owns retry, to avoid
multiplying attempts. Splits that belong to the same input must share the unit
of work so failure reaches the input's terminal outcome.

Known split limitations in 0.3.x: split parts keep the input's message id, and
a send node has one step id, so all parts share one idempotency key. With
`basis.message-store.step-outcome-store=postgres`, only the first part sent
through a node is delivered; later parts are `SKIPPED`, also in the first
attempt. From 0.3.6 such a skip is logged at ERROR in the attempt that sends
the first part; see
[Splitting before a tracked send in 0.3.x](#splitting-before-a-tracked-send-in-03x).
Without `stopOnException()`, each failed part is parked separately and
the input still records `COMPLETED`; with it, the first failure parks the input
once and records `DLQ`. Per-part identity is not part of 0.3.x; 0.4 adds it. See the
[split step identity advisory](docs/split-step-identity.md) for the exact
conditions under which a part is not sent, and the safe configuration.

Sends in a sub-route: from 0.3.7, a tracked send in a route that another send
calls, such as a `direct:` sub-route, records its outcome once per try, like a
send in the input route. The calling `to("direct:…")` is internal and records
nothing. When the sub-route's dead letter channel parks the message after the
send failed, the step stays `FAILED`, so a later read of the record, a resend
or a replay sends it again. The same holds when the sub-route has no error
handler of its own and the input route's handler retries the call.

A later read can still follow a parked message. A commit fails after the DLQ
publish when a receiver that accepts the connection and never answers kept the
route busy for longer than the consumer's `max.poll.interval.ms` (5 minutes by
default; with the `http` component's default timeouts, four tries take about
12 minutes). The record is then read again, its route runs again and parks one
more copy, for as long as the receiver hangs. When the receiver answers again,
that read delivers the message and records it `COMPLETED`; the copies already
parked stay on the DLQ topic, and a resend of one skips the delivered step. Keep
a receiver call's timeouts times its tries well under the poll interval, as
described below for the processing time budget and the `http` component's
timeout options.

Adapter 0.3.5 and 0.3.6 also recorded the sub-route's step when the calling
send completed
([#48](https://github.com/basis-no/integration-platform-adapter-camel-quarkus/issues/48)).
A successful call was listed twice as `DELIVERED`. When the sub-route's dead
letter channel parked the message after the send failed, the calling send saw
no failure and recorded the step `DELIVERED` with no error. A resend or replay
then skipped the send, and the receiver never got the message. With the
`postgres` step store, any later read of a parked record that carries
`x-bip-message-id` skipped it too and, when the rest of the route succeeded,
recorded the message `COMPLETED`. Such a read came from:

- a commit that failed after the DLQ publish, as above;
- a rebalance that took the partition away before the commit;
- a crash between the DLQ publish and the commit.

A message that 0.3.5 or 0.3.6 recorded this way keeps its `DELIVERED` step
after the upgrade, so it is still skipped. To find these messages:

1. Read the `x-bip-message-id` header of the records on the DLQ topic
   (`bip.message.dlq` unless `basis.message-store.dlq-uri` names another), as
   far back as its retention reaches, with a Kafka tool that prints headers,
   for example
   `kafka-console-consumer.sh --bootstrap-server <host:port> --topic bip.message.dlq --from-beginning --property print.headers=true --property print.value=false --timeout-ms 10000`.
2. Look up those ids in the Message Store. The ones to check are `COMPLETED`
   although they have a DLQ record:

   ```sql
   select message_id, status, last_attempt, occurred_at
   from bip_message_log
   where integration = ?
     and status = 'COMPLETED'
     and message_id in ('<id from the DLQ>', '<another id>');
   ```

   A message that was parked and later resent with success is listed too. A
   row with `last_attempt` = 1 was not resent through the adapter's replay;
   step 3 tells whether it is this case.

3. For each of them, read its attempts in `bip_message_attempt` (see
   [Read Model](#read-model)). A sub-route step listed as `SKIPPED` in
   `steps_json` points to this case. Check at the receiver whether it got the
   message.
4. Resend a message the receiver did not get with the step in `redoSteps`,
   together with any step whose output it uses (see
   [Redo behavior](#redo-behavior)). Redo repeats the call with the same
   `Idempotency-Key`.

Every message that 0.3.5 or 0.3.6 parked after a failed send in a sub-route
has that step recorded `DELIVERED`, also when it is still in the DLQ: resend it
with the step in `redoSteps`.

Message Store outages in 0.3.x
([#84](https://github.com/basis-no/integration-platform-adapter-camel-quarkus/issues/84),
[#66](https://github.com/basis-no/integration-platform-adapter-camel-quarkus/issues/66)):
while the Message Store cannot be written, a Kafka record's outcome cannot be
recorded, so its offset is not committed and the consumer reads the record
again, for as long as the outage lasts. Up to 0.3.6 every read runs the route
with its redeliveries (by default 3, after 1, 2 and 4 seconds) and logs the
failure at ERROR with its stack trace. From 0.3.7 a failed step outcome check
or write ends the read at once, without redeliveries. The partition waits; no
record is committed early.

From 0.3.7 the record is read again with a growing wait. This applies to every
record whose outcome could not be made durable, because the outcome write, a
step outcome check or write, or the DLQ publish failed. The first time, it is
read again at once. If the same record comes back with its outcome still not
durable, the adapter waits before handing it back: 1 second, doubling on each
further read, at most 30 seconds. The wait is also kept to half the consumer's
effective `max.poll.interval.ms` (an `additionalProperties` value overrides the
named option) less the time since the records of the poll that returned it
began to be processed, including the records before it, so that the consumer
polls again within its interval. This relies on `break-on-first-error=true`,
the adapter's default, under which a record that is read again ends its poll;
without it there is no wait. A committed outcome on the partition, or a later
record, starts again at once. The consumer waits for it, also when the route
completes on another thread, so the consumer's other partitions wait too, as
they do after any record that is read again. The wait ends early when the
application stops. A record whose topic, partition and offset the adapter did
not see at the start, or one without the adapter's manual commit, is read again
at once. Up to 0.3.6 every record was read
again at once, about twice a second, for as long as the failure lasted, and
each read ran the route again, so the log grew quickly.

- With `basis.message-store.step-outcome-store=postgres`, a tracked send
  checks the store before it runs. While the store cannot be read, that check
  fails before the send reaches its receiver, so the send is not repeated. A
  send that ran just before the store went down, whose `DELIVERED` outcome could
  not be recorded, can run once more after the store is back; from 0.3.7 only
  after a restart or a rebalance to another process, since the outcome is kept
  in memory until it can be written. With the `noop` step store, every read
  runs the route's sends again.
- **When the store can be read but not written, 0.3.6 and earlier repeat a
  tracked send on every read, without end.** Examples: the database is
  read-only (after a failover onto a standby, or for maintenance), or the
  integration's database role has lost its write privilege; a full disk
  probably does the same. With the `postgres` step store, on every read:
  - the store check passes, and the send reaches its receiver;
  - its `DELIVERED` outcome cannot be written, so the offset is not committed;
  - the next read sends again. The route's redeliveries can also repeat the
    send within one read.

  Up to 0.3.6 one message could be sent dozens of times a minute. From 0.3.7
  the outcome write is tried again in place, and an outcome that still cannot
  be written is kept in memory, so the record's next read in the same process
  writes it and does not send again; a store failure also ends the attempt, so
  no redelivery repeats the send. See
  [A send whose `DELIVERED` outcome could not be written](#a-send-whose-delivered-outcome-could-not-be-written).
  A restart, or a rebalance to another process, loses what was kept, so the
  next read there sends once more. Readiness stays `UP`. Up to 0.3.6 the ERROR
  log says that the message was not parked because a step outcome was not
  checked or recorded, with the database's error. From 0.3.7 that line is gone,
  since the dead letter channel no longer runs for such an attempt; the ERROR
  line is `Message <id> is not committed: a step outcome was not checked or
  recorded, so it is consumed again`, with the database's error, and it ends
  with `storeError=read-only` or `storeError=privilege` and the SQLSTATE. For a
  record that carries `x-bip-message-id`, every repeat has the same
  `Idempotency-Key`, so only a receiver that deduplicates by that key is safe.
  A record without that header gets a new message id, and so a new key, on
  every read up to 0.3.6; deduplication by key does not stop its repeats. From
  0.3.7 it keeps its generated id while it is read again in the same process,
  and gets a new one after a restart or a rebalance.
- With the `postgres` step store, a message that fails at a tracked send's
  store check is not parked, and the record is read again. Up to 0.3.6 the
  adapter logs at ERROR that it was not parked because a step outcome was not
  checked or recorded; from 0.3.7 it logs the `is not committed` line above,
  and neither the dead letter channel nor an application's own `onException`
  runs for that attempt.
- With the `noop` step store, or when the route fails before its first tracked
  send (in a processor, a validation or an internal endpoint), a failing message
  is parked in the DLQ on each read, before its outcome write fails again, so
  the DLQ can hold several copies of it. Copies of a record that carries
  `x-bip-message-id` have that id; a record without it gets a new id on every
  read, so its copies have different ids. From 0.3.7 the copies come at most at
  the pace of the growing wait above. After the store is back, its last read
  may complete or park it once more.

What to do:

- When the store can be read but not written, scale the integrations that use
  it down to zero replicas at once, without waiting. Up to 0.3.6 every read
  until then repeats their outside sends; from 0.3.7 their partitions wait, and
  a restart or a rebalance can repeat a send once. The ERROR log shows a
  database error such as `cannot execute INSERT in a read-only transaction` or
  `permission denied for table bip_step_outcome`, and from 0.3.7
  `storeError=read-only` or `storeError=privilege`. Scale them back up once
  writes work again. Receivers should deduplicate by `Idempotency-Key`; this helps
  only for records that carry `x-bip-message-id`.
- From 0.3.7 each statement of the Postgres sink and the Postgres step outcome
  store is bounded by `basis.message-store.statement-timeout` (10 s) and
  `basis.message-store.lock-timeout` (5 s), and a call by a network timeout, so
  a held lock or a database that stopped answering fails the call instead of
  holding the consumer thread past `max.poll.interval.ms`; see
  [Message Store call timeouts](docs/configuration.md#message-store-call-timeouts).
  Set `loginTimeout` in the JDBC URL as well, for example `?loginTimeout=10`.
- When the store cannot be read either, and the outage lasts longer than a
  few minutes, scale the integration down to zero replicas until the store
  accepts writes again, then scale it back up. Its uncommitted records are read
  again from the committed offsets.
- Keep `auto.offset.reset` at `earliest`, the adapter's default from 0.3.7, on
  every Kafka input; on 0.3.6 and earlier, set
  `camel.component.kafka.auto-offset-reset=earliest` yourself. A partition that
  has no committed offset yet starts where the consumer's `auto.offset.reset`
  says, and Camel's default is `latest`: after a restart or a rebalance, the
  consumer would start after a record that was never committed. From 0.3.7 a
  managed Kafka input with another value logs a warning at startup; it is not
  refused.
- After an outage, before resending anything from the DLQ, check it for copies
  of the same message (the same `x-bip-message-id`; for records that arrived
  without one, the same payload), check the message's latest status in the
  Message Store, and resend each message once, only if it did not complete.

A pause while the store is down is planned for 0.4, not released; it is not
part of 0.3.x.

Processing time still needs an application-specific budget: bounded receiver
timeouts and retries, number of transactions per input, poll batch size and
`max.poll.interval.ms` must fit together. This patch does not classify every
HTTP 500 as permanent or automatically resize application memory.

On Camel 4's `http` component, bound a receiver call with `responseTimeout`
and `connectTimeout`. Camel accepts some timeout options without applying them,
and the call then waits for the component's timeout, three minutes unless
configured. Checked on Camel 4.18.0 and 4.18.4:

| Option | Where | What Camel does | Use instead |
| --- | --- | --- | --- |
| `socketTimeout` (Camel 3) | any `http`/`https` URI | Sends it to the receiver as a query parameter | `responseTimeout`; on a dynamic send, `httpClient.responseTimeout` |
| `httpConnection.socketTimeout`, `.responseTimeout`, `.connectTimeout`, `.connectionRequestTimeout` | any `http`/`https` URI | Drops it without an error; only `httpConnection.soTimeout` applies | `responseTimeout`, `connectTimeout`, `connectionRequestTimeout`; on a dynamic send, the `httpClient.` form |
| `responseTimeout`, `connectTimeout`, `connectionRequestTimeout`, `soTimeout` | dynamic send: a `toD`, an `enrich`, or a `wireTap` whose URI contains `${…}` | Sends it to the receiver as a query parameter | `httpClient.responseTimeout`, `httpClient.connectTimeout`, `httpClient.connectionRequestTimeout`, `httpConnection.soTimeout` |

A dynamic send keeps only the options that Camel's endpoint metadata lists, and
the metadata lists these four timeouts as component options only, so the rest
go to the receiver. `allowOptimisedComponents(false)` on a `toD` or `enrich`
turns that off, at the cost of one endpoint per distinct URI. The `httpClient.`
forms, `httpConnection.soTimeout` and the component settings
`camel.component.http.response-timeout`, `connect-timeout`,
`connection-request-timeout` and `so-timeout` apply to every send. Some forms
fail on their own: `httpClient.socketTimeout` and `httpClient.soTimeout` when
Camel creates the endpoint, which is at startup for a `to` and at the first send
for a dynamic send, and `camel.component.http.socket-timeout` at startup.

From 0.3.6 the adapter logs a warning for each option in the table when it
creates a route with it in a `to`, `toD`, `enrich` or `wireTap` URI, once per
route, step and option. The warning names the option, the step and the
replacement, never the URI or a value. A URI in a `recipientList` or
`routingSlip`, or one whose scheme is only known at send time, is not checked.

A timeout does not mean the receiver did nothing. A receiver can apply the
effect after the client gave up, and the message is then retried and parked
although the effect happened. The step is recorded `FAILED`, so a replay sends
it again, with the same `Idempotency-Key` (`<message-id>:<step-id>`) as every
earlier try. Only a receiver that deduplicates on that key avoids a second
effect. Republishing the parked record under a new message id changes the key,
so even such a receiver applies it again.

The DLQ record has the shape listed below. It also carries the other message
headers present at failure, because the Kafka producer forwards them. From 0.3.6
the values of credential-like headers are masked as `xxxxxx`: `Authorization`,
`Cookie`, `Set-Cookie`, `WWW-Authenticate`, and names containing secret, token,
password, credential, session, signature or auth, or ending in `-key` or `_key`. The
names stay visible. Remove any other receiver header you do not want on the
shared DLQ before the failing step.

An HTTP intake that publishes to Kafka must remove the caller's headers first,
for example with `removeHeaders("*")` and then setting only the headers the
message needs. Camel copies every request header onto the exchange, and the
Kafka producer publishes them with the message, including `Authorization`, API
keys and cookies, unmasked. Anyone who can read the topic can read them. From
0.3.6 a Kafka send that published a header with a credential-like name, by the
masking rule above, logs a warning once per route and header name. The warning
names the header, never its value, and says how to remove it. What is published
does not change. The check runs after a successful `to` or `toD`, against the
endpoint the send used, including a `toD` that chooses its component only when
it sends. It follows that endpoint's header filter strategy and counts only
text, byte array, `Long`, `Integer`, `Double` and `Boolean` values, which
Camel's header serializer writes as they are. An endpoint whose header filter
strategy or header serializer is an application class, or whose filter holds an
application collection, is not checked: the adapter does not call application
code to find out what was published. Nor is a send through a `toD` with
`cacheSize=-1` to an endpoint that is not yet registered: Camel creates such an
endpoint for the one send without registering it, so the adapter cannot find it
afterwards. An endpoint that is already registered is reused and checked. At
most 100 header names are reported per route; one more line then says that
further names are not logged. Also from 0.3.6, a tracked send through Camel's
`http` component does not forward credential-like headers that still hold a
consumed record's bytes, and logs a
warning that names the header. A `toD` that chooses its component only when it
sends, such as `${header.receiverUrl}`, is treated as an HTTP send. Other record headers still reach the receiver,
as byte-array text unless they are `x-bip-*` headers.

**Known issue
([GHSA-9xmf-gp9j-qw7w](https://github.com/basis-no/integration-platform-adapter-camel-quarkus/security/advisories/GHSA-9xmf-gp9j-qw7w)):**
on camel-kafka versions before 4.14.8, 4.18.3 and 4.21.0, which include
Camel 4.18.0 from Quarkus 3.32.2, Camel's Kafka consumer also sets headers
named `kafka.*`. One of them, `kafka.HEADERS`, holds the record's whole header
set. Camel's `http`, `vertx-http` and `netty-http` producers send it to the
receiver as a header whose value lists the bytes of every record header,
credentials included. The withholding above does not cover it, and there is
no 0.3.x fix. Remove that one header before an HTTP send:

```java
.removeHeader("kafka.HEADERS")
.to("https://receiver.example/orders").id("post-order")
```

The consumer's other `kafka.*` headers, such as the record key, topic,
partition, offset and timestamp, still reach the receiver, but they hold no
other header's value. To remove every header but the adapter's, see
[Removing headers before a tracked send](#removing-headers-before-a-tracked-send):
`removeHeaders("*")` alone also removes `x-bip-message-id`, and with it the
send's stable step key. Do not remove the `kafka.*` headers with `removeHeaders("kafka.*")`:
that also removes `kafka.KEY`, which Camel's Kafka producer uses as the key of
the record that the adapter parks in the DLQ, so a parked record loses its
key. Alternatively, upgrade to a Quarkus platform that brings camel-kafka 4.14.8, 4.18.3 or
4.21.0 or later, such as Quarkus 3.33.4. Those versions name the header
`CamelKafkaHeaders`, which Camel's own HTTP header filters drop; an endpoint
with its own `headerFilterStrategy` that lets `Camel*` headers through still
sends it, so remove `CamelKafkaHeaders` before such a send or make that filter
drop it. The withholding above covers only a
tracked `to` or `toD` whose endpoint URI starts with `http:` or `https:`,
which includes a static `to` through `netty-http`. So also remove the record's
credential headers, such as `Authorization`, before every other send to an
HTTP receiver:

- a send through `vertx-http`, which sends them as text, or a `toD` through
  `netty-http`, which sends them as lists of numbers;
- an `enrich`, `wireTap`, `recipientList`, `routingSlip` or `ProducerTemplate`
  send;
- with adapter 0.3.5 and earlier, every HTTP send.

Adapter 0.4.0, not yet released, will withhold the `kafka.*` headers and the
record's credential headers on sends to `http`, `https`, `netty-http` and
`vertx-http` endpoints, through any EIP, with documented limits; see its
changelog.

- the body is the **original consumed payload**, restored from the payload
  captured at route start — not a mid-transformation body, and not a byte array
  the route changed in place. The adapter keeps one copy of a `byte[]` input:
  the inline replay copy, or, when the payload is not stored for replay
  (`metadata_only` mode, replay disabled or above the size cap), a copy kept
  only for parking. A `String` input is not copied. When the body type cannot be
  captured, the current body is parked as-is.
- all `x-bip-*` envelope headers are preserved, plus two diagnostics headers
  set at parking time: `x-bip-error` (the failure message) and `x-bip-dlq-at`
  (RFC 3339 timestamp). From 0.3.6 a replay's `x-bip-redo-steps` is not among
  them; see [Redo behavior](#redo-behavior).
- the attempt is recorded with status `DLQ`, and the Kafka offset is committed
  only after the outcome record (and DLQ publish) succeeded.

### A send whose `DELIVERED` outcome could not be written

From 0.3.7, with the `postgres` step store, when a send returned but its
`DELIVERED` outcome cannot be written, for example because the Message Store is
read-only after a failover or the login lost its write privilege, the send is
not made again only because its outcome was lost:

1. On the thread that processes the record, and outside an application
   transaction, the adapter writes the same outcome again in place, up to 3
   more times after 0.5, 1 and 2 seconds, starting no try more than 10 seconds
   after the first failure. Each try is bounded by the
   [Message Store call timeouts](docs/configuration.md#message-store-call-timeouts),
   so when the store stops answering the write takes up to about 40 seconds in
   all at the defaults (about 30 when every try runs to the network timeout). A write that succeeds lets the attempt go on.
2. If it still fails, the adapter keeps a mark in memory for the input route,
   the record's topic, partition and offset, and the step key. The attempt
   fails, and the record is not committed and is read again with the growing
   wait above.
3. When the same record is read again, a step whose step key has a mark gets
   its outcome written from the mark before the store check. If that
   succeeds, the send is skipped, the step is listed as `DELIVERED` with the
   time of the send, and a WARN says so. If it fails, nothing is sent and the
   record is read again later. A mark wins over a redo list: the earlier read
   of the same record already made that send.

A failed store call ends the attempt: every send through `to` or `toD` after it
in the same attempt fails before it sends, with the same failure, tracked or not
(such as a `direct:` call), including a redelivery of the same send and a later
send after a `doCatch`. Other processing goes on as the route says, but nothing
is committed. Sends already running in parallel parts still finish.

A record without `x-bip-message-id` keeps the id generated for it while the
same offset is read again in the process, so its marks match. Split parts with
their own `x-bip-message-id` have their own marks; a part whose id is random on
every read never matches. Marks are kept in memory only, at most 10,000 of
them (a dropped one is logged), and are dropped when a record at or after their
offset is committed, or when a record at any other offset of the partition
begins. A restart, or a rebalance to another process, loses them, so the next
read there can send once more with the same `Idempotency-Key`. A mark whose
step the next read does not reach, because the route takes another path, is
not written. When parts of one attempt share a step key, only the part that
reaches it first is written from the mark; the others are skipped and reported
as not delivered, as for a ledger skip. Marks and kept ids match by offset only,
so after a topic is deleted and created again, restart the integrations that
consume it.

The adapter's ERROR lines for a record that is not committed end with
`storeError=<class>` and the SQLSTATE, so an alert can tell these cases apart:
`read-only` (`25006`), `privilege` (`42501`, class `28`), `schema` (another
class `42`, class `23`, `3D000`, `3F000`), `record-data` (class `22`, `54000`;
see [A record whose own values the store cannot hold](#a-record-whose-own-values-the-store-cannot-hold))
and `unavailable` (everything else, including timeouts).

### A record whose own values the store cannot hold

A record whose envelope values PostgreSQL cannot store would otherwise block
its partition: every write for it fails, so it is neither recorded nor parked.
From 0.3.7, before the route runs, the adapter checks the raw bytes of the
headers the store keeps as text, `x-bip-message-id`, `x-bip-correlation-id`,
`x-bip-content-type`, `x-bip-source-system` and `x-bip-target-system`, and
refuses a value that contains a NUL byte or is longer than 1,024 bytes. Header
names are matched ignoring case. A refused record's route does not run, so
nothing is sent. This applies to a managed Kafka input, one with the adapter's
manual commit.

The same terminal path runs when the step outcomes that failed all failed with
a SQLSTATE of class `22` or `54000` for the record's own data, for example an id
the route set itself that is too long for the step ledger's index, also when
the route caught the failure, or when the outcome write fails so while every
step outcome was written. When a step outcome failed for another reason, the
record is read again instead:

1. The record is parked, unless it was parked already or, for the outcome
   write, its route completed. `x-bip-error` gives the reason, which header was
   refused and why or the SQLSTATE, says whether a send had returned, and ends
   with the record's position as `(record <topic>/<partition>@<offset>)`; the
   refused value is not copied into it.
2. A reduced outcome is recorded under the record's own `x-bip-message-id`,
   with its own attempt, `DLQ` (or `COMPLETED` when its route completed), the
   payload as captured, the reason as its error, and no correlation id, content
   type, source or target system or steps. No step outcome is written. If the
   store refuses that too, it is written without the payload, not replayable,
   and the record is not parked again on its next reads in the process.
3. The offset is committed only after both are durable; otherwise the record is
   read again and the path runs again. The consumer then goes on with the next
   record, and an ERROR line says `Message <id> from record <position> was
   parked: <reason> storeError=record-data`, or, when the route completed and
   nothing was parked, `... was recorded without its envelope values: <reason>
   storeError=record-data`.

When `x-bip-message-id` itself is refused, the record is parked and recorded
under an id derived from its position: the UUID made from the first 16 bytes of
SHA-256 over `kafka-record`, the integration name, the topic, the partition and
the offset, joined by line feeds, with the version and variant bits of a
name-based UUID. The .NET adapter derives the same id.

After three records in a row on a partition took this path because the store
refused their data (records the boundary check refused do not count), the next
one is held back, not committed, with `storeError=schema`, so that a setting or
column that fails every record does not drain the topic into the DLQ. The
held-back record is read again with the growing wait and goes on once the store
takes it, for example after the setting or column is fixed. A normal commit on
the partition resets the count, and a restart does too: if the records really
carry data the store cannot hold, each restart parks three more of them.

A failure text the adapter stores has U+0000 replaced with U+FFFD, so that a
failure's own message cannot make its step or record unstorable.

## Read Model

The direct Postgres sink writes two tables for operator views:

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

`steps_json` is deliberately JSONB for now. If operator views need richer
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

The adapter stores replay-eligible input payloads inline in Postgres by default:

- `payload_storage = 'postgres_inline'`
- `payload_size_bytes` records the original incoming body size when the adapter
  can determine it safely.
- `payload_bytes` stores the original incoming body for `String` and `byte[]`
  messages when it is within the configured cap.
- `payload_expires_at` is set from the payload retention policy.
- `replayable` is true when the Message Store currently has the payload and
  policy allows manual replay.

The adapter deliberately does not consume stream-like or unknown body types just
to capture payload bytes. Those messages fall back to:

- `payload_storage = 'metadata_only'`
- `replayable = false`
- `replay_disabled_reason` explaining why replay is unavailable.

Payload retention is separate from metadata retention. A future cleanup job can
clear `payload_bytes`, set `payload_deleted_at`, flip `replayable` to false, and
keep the audit metadata in place.

Payload storage captures the incoming body at route start. It currently stores
`String`, `byte[]`, and `null` bodies. Stream-like or unknown body types are not
consumed by the adapter just to make replay possible.

## Manual Replay

The adapter includes `MessageReplayService`, backed by Postgres payload storage and Kafka
republish. It loads a replayable row from `bip_message_log`, validates that
payload bytes are still available, writes an `bip_message_replay_audit` row, and
publishes the stored payload back to the integration input topic.

The adapter also exposes an admin endpoint that an operator's tool can call:

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
  `redo_steps` audit column

By default, the input topic is derived from
`basis.message-store.replay.input-topic-pattern`, default `bip.%s.in`, where `%s`
is the stored integration name. Test/demo integrations can override this to a
fixed topic.

### Redo behavior

Replay does not bypass idempotency by default. If
`basis.message-store.step-outcome-store` is `postgres`, already delivered steps
with the same `<message-id>:<step-id>` idempotency key are skipped and recorded
as `SKIPPED` on the replay attempt.

`x-bip-redo-steps` is the explicit operator override: the
step tracker parses the header as a JSON array of step ids and re-executes
exactly the listed steps even when the durable ledger already has them
`DELIVERED`. A step matches when the list contains its step id or its full
idempotency key. A malformed header value is logged as a warning and treated as
empty — it never fails message processing. Redo intentionally repeats an
external side effect, so external systems still need domain idempotency for it.

From 0.3.6 the list stays inside the integration. When an exchange enters a
route, the adapter reads `x-bip-redo-steps`, keeps the list for the attempt's
step tracking and removes the header: a Kafka input with the Message Store
before the route runs, and every other route through a route policy the adapter
adds. The route's sends therefore do not carry it, whether through `to`, `toD`,
`enrich`, `wireTap`, `recipientList` or `routingSlip`. A handler that restores
the original message, for example with `useOriginalMessage()`, gets the header
back; the next `to` or `toD` and DLQ parking remove it again. Before, a receiver
got the list, and a downstream integration that consumed a published record
with the same message id sent its own steps with the same ids again.
`x-bip-replay` and `x-bip-attempt` are still forwarded; nothing in the adapter
acts on `x-bip-replay` at a Kafka input. The warning for a malformed list names
the header only, not its value.

## Step Tracking

The adapter observes Camel endpoint sends with a Camel `InterceptStrategy`. For
stable step names, use normal Camel ids on important external sends:

```yaml
- to:
    id: deliver-vendor-order
    uri: "sftp:vendor/orders?fileName=order-${header.x-bip-correlation-id}.xml"
```

Every tracked external send must have an explicit id. Without one, Camel
generates a node id such as `to1`. It changes when the route is edited, and other
integrations generate the same ids: when a message id reaches another
integration on the same Message Store, the second integration's
`<message-id>:to1` matches the first one's delivered step and its own send is
skipped. From 0.3.6 the application therefore fails to start when a tracked send
(`to` or `toD` to an external endpoint) has no explicit id. Internal endpoints,
wire taps and the adapter's own dead letter route are not checked.

`basis.message-store.step-ids.emergency-allow-generated=true` is an emergency
escape hatch only: the application then starts and logs a warning for each such
send. Use it to restore service while the ids are added, not as a setting.

The startup check covers the routes as Camel builds them, so it also sees
endpoints whose URIs are built elsewhere, such as placeholders and constants,
which a check of the route source misses.

### Upgrading from generated step ids

Keep each send's current key: give it the id Camel generates for it today, for
example `.to("sftp:...").id("to1")` or `id: to1` in YAML. The step key stays
`<message-id>:to1`, and so does the receiver's `Idempotency-Key`. Messages
recorded before the upgrade are then skipped on replay, and a redo repeats them
under the same key. The startup check accepts any explicit id, including one
that looks generated.

Renaming a generated id, for example `to1` to `send-to-sap`, changes that step's
key: replaying, redoing or resending a message recorded before the change sends
it once more. Rename only once no message recorded under the old id will be
processed again, for example after the message retention period. A kept `to1`
is still the same id other integrations generate, so the advice on separate
message ids above still applies.

Generated numbers count per node kind (`to`, `toD`, ...) across every route in
the application, in the order Camel builds them. They include the adapter's dead
letter route, which holds one `toD`. Counting in the source is therefore only a
guess. Confirm the ids before the upgrade:

- With 0.3.6 in a development or test environment, start the unchanged
  application once with `basis.message-store.step-ids.emergency-allow-generated=true`.
  Each tracked send without an id is logged as a warning with its route,
  endpoint and generated step id. The adapter's routes did not change from
  0.3.5, so the ids are the same as on 0.3.5.
- On 0.3.5, add `camel-quarkus-yaml-io` and set `camel.main.dump-routes=yaml`,
  `camel.main.dump-routes-generated-ids=true` and
  `camel.main.dump-routes-resolve-placeholders=false`. The routes are logged at
  startup with their node ids. Without the last setting the dump can contain
  resolved credentials.
- In the Message Store, list the step ids an integration has recorded. Rows
  recorded before 0.3.7 can contain endpoint credentials (see the stored
  endpoint rule below); this query masks the common forms:

  ```sql
  select o.step_id,
         regexp_replace(regexp_replace(o.endpoint_uri, '//[^/@]*@', '//xxxxxx@'),
           '(?i)([?&](code|sig|key|pwd|[^=&]*(password|passphrase|passcode|secret|token|signature|credential|auth|apikey|api-key|api_key|[-_]key|accesskey|accountkey|privatekey|connectionstring)[^=&]*)=)[^&]*',
           '\1xxxxxx', 'g') as endpoint,
         count(*) as steps, max(o.occurred_at) as last_occurred_at
  from bip_step_outcome o
  where o.message_id in (select a.message_id from bip_message_attempt a
                         where a.integration = 'order-pipeline')
  group by o.step_id, 2
  order by o.step_id, last_occurred_at desc;
  ```

  A step id listed with two endpoints changed meaning when the route was edited;
  the latest rows show the current one.

Prefer a new `x-bip-message-id` per integration and carry continuity in
`x-bip-correlation-id`. Explicit ids must still be unique across the
integrations that can see the same message id.

If no node id is available at all, the adapter derives a fallback key from route
and endpoint metadata. Fallback keys are acceptable for audit, but explicit ids
are better for operator display and future partial replay.

Internal Camel endpoints such as `direct:`, `log:`, `bean:`, `mock:`, `seda:`,
`timer:`, and `micrometer:` are ignored. External sends get:

- `x-bip-idempotency-key = <message-id>:<step-id>`
- `Idempotency-Key = <message-id>:<step-id>`

The adapter records `DELIVERED`, `FAILED`, or `SKIPPED` step outcomes in the
message attempt. `SKIPPED` means the configured `StepOutcomeStore` says the same
idempotency key was already delivered, so the endpoint send was not repeated.
From 0.3.6 each skip is logged at INFO with the step, message id and masked
endpoint, and the logging sink counts delivered, skipped and failed steps
separately. A skip that loses a split part, loop iteration or branch is logged
at ERROR instead; see
[Splitting before a tracked send in 0.3.x](#splitting-before-a-tracked-send-in-03x). A route log placed after a send cannot tell by itself whether the send
happened. Directly after the send, the exchange property
`basis.messageStore.stepAlreadyDelivered`
(`PlatformExchangeProperties.STEP_ALREADY_DELIVERED`) is `true` only when that
send was skipped. Every later `to`/`toD`, including `log:` and `direct:`, resets
it, so read it in the step that follows the send, for example a `choice` or
`setProperty`. Do not log "delivered" without checking it.

Each recorded step keeps its endpoint URI, in `bip_step_outcome.endpoint_uri`
and the attempt's `steps_json`, with its credentials masked as `xxxxxx`. From
0.3.7 the rule is the one the .NET adapter applies to the steps it records, so
both adapters store the same value for the same URI:

- what Camel's `URISupport.sanitizeUri` masks: the value of every query
  parameter whose name contains one of Camel's sensitive option names (such as
  `password`, `token`, `secret`, `apikey`, `user` or `clientId`), and the
  password in a `user:password@` part, including inside a parameter value;
- the whole user info of the endpoint's authority (`https://xxxxxx@host/…`),
  not only its password;
- the value of every query parameter with a credential-like name, by the rule
  the adapter uses for the headers it masks in a DLQ record: `Authorization`,
  cookies, authentication challenges, names containing secret, token, password,
  passwd, credential, session, signature, apikey, api-key, api_key,
  subscription-key, private-key or auth (such as `Signature` or
  `X-Amz-Credential`), or ending in `-key` or `_key` (such as
  `subscription-key`), but not `x-bip-*` or `Idempotency-Key`; also the names
  `key`, `code`, `sig` and `pwd`, and names containing `passphrase`,
  `passcode`, `privatekey`, `accesskey`, `accountkey` or `connectionstring`.
  Parameter names are compared after percent-decoding (an escape that is not
  valid UTF-8 is compared as written), ignoring case.

A run of masked characters becomes one `xxxxxx`. Keywords an application adds
with `URISupport.addSanitizeKeywords` are masked too (the .NET adapter has no
such setting). An endpoint the adapter's patterns cannot mask within about a
second is stored as `xxxxxx`; when an application has added keywords, Camel's
own sanitizer also runs on it, and that pass has no such bound. Up to 0.3.6
only Camel's own sanitizer ran: a function key `?code=…`, a signature
`?sig=…` and the user name in `sftp://user:pass@host` were stored in clear.

Step ids are derived from the exact URI, so masking does not change them. The
route still sends to the real endpoint; only the stored and logged copy is
masked. Failed and skipped step entries use the same masked endpoint, as do the
warning emitted when a send has no stable Camel node id and the startup
refusal for a send without an id. This masks the endpoint field, not arbitrary
exception messages or application logs. Existing rows, logs and backups are not
rewritten, unless the same step is recorded again; review their access and
retention separately. A query or tool that matched a stored endpoint's user
name or one of these parameter values no longer finds it in rows recorded from
0.3.7.

### Removing headers before a tracked send

A tracked send in a Kafka input route builds its step key
`<x-bip-message-id>:<step-id>` and its `Idempotency-Key` from the exchange's
`x-bip-message-id` header at the moment it sends. A route that removes every
header first, for example
`removeHeaders("*").to("https://receiver.example/orders").id("send-order")`,
also removes that header. The send then generates a new random id and logs a
warning (`Message missing x-bip-message-id; generated <id>`)
([#143](https://github.com/basis-no/integration-platform-adapter-camel-quarkus/issues/143)):

- The step key and the `Idempotency-Key` change on every delivery. A
  redelivery, a read again, a resend or a replay sends again under another key,
  so the step store never skips the send, and even a receiver that
  deduplicates by `Idempotency-Key` applies it twice.
- A message whose send completes is journaled under the random id, so the
  consumed message cannot be found or resent by its own id. The fallback to
  the consumed input's id that 0.3.6 added applies only when no tracked send
  follows the removal: the send sets the generated id as the header, and that
  value is journaled. A message that fails and is parked is still parked and
  journaled under the consumed input's id.

Keep the adapter's headers when you remove the others:

```java
.removeHeaders("*", "x-bip-.*")
.to("https://receiver.example/orders").id("send-order")
```

`x-bip-message-id` is the header a tracked send needs. The other `x-bip-*`
headers are the message's envelope; the adapter reads them when the input
arrives and journals the values it read then, but keeping the whole prefix is
the simplest rule. The adapter sets `x-bip-idempotency-key` and
`Idempotency-Key` itself at each tracked send, and reads a replay's
`x-bip-redo-steps` when the input arrives, so removing those does not change
the send.

Removing every header also removes Camel's Kafka record key header:
`kafka.KEY` on camel-kafka versions before 4.14.8, 4.18.3 and 4.21.0, such as
4.18.0, and `CamelKafkaKey` from those versions, which rename the `kafka.*`
headers. Camel's Kafka producer uses it as the key of a record the adapter
parks in the DLQ, so keep it too if a parked record should keep its key:

```java
.removeHeaders("*", "x-bip-.*", "kafka.KEY", "CamelKafkaKey")
```

Both forms still remove the record's credential headers, such as
`Authorization`, and `kafka.HEADERS` (see [Retry and DLQ](#retry-and-dlq)). A
record parked after the removal carries only the headers that were kept, so
also keep any application header a parked record must carry.

Inside a `split`, do not rely on removed headers to give each part its own
step key: the generated ids change on every delivery, so every part is sent
again on each redelivery and resend. Use one of the safe forms in
[Splitting before a tracked send in 0.3.x](#splitting-before-a-tracked-send-in-03x).

Keeping the consumed record's id when a route removes it is tracked in
[#143](https://github.com/basis-no/integration-platform-adapter-camel-quarkus/issues/143);
0.3.7 does not change the send.

### Splitting before a tracked send in 0.3.x

A step key is `<x-bip-message-id>:<step-id>`. Split parts, loop iterations and
multicast and recipient list branches keep the input's message id, so parts
that reach the same tracked send share one key. With
`basis.message-store.step-outcome-store=postgres`, only the first part is sent.
Each later part's send is skipped as already delivered, and that part's data
does not reach the receiver. The input still completes: a Kafka offset is
committed, and a polling route goes on to mark its batch processed. This is the
same in every 0.3.x release.

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

The log message is one line. It names the step, the input route and the kind of
part, and contains no message content or header values. Only the log changes:
the skip, the input's outcome, retries, parking and commits are as before. The
line is written only when it is certain: a part at another position in the
enclosing splits, loops, multicasts and recipient lists sent the key in this
attempt, and this part did not. Every other skip stays at INFO
(`Step <step-id> SKIPPED for message <id>`). That includes every part of a
redelivered or replayed input whose earlier attempt sent the key, because the
adapter cannot tell which part's data that attempt sent. It also includes a part
that meets its own delivered send again when Camel repeats a sub-route or a
whole split. The `noop` step store never skips, so every part is sent and
nothing is logged.

Safe forms, which send every part:

1. Give each part its own `x-bip-message-id` before the send, derived from its
   record so that a replay keeps the same key:

   ```java
   .split(body().tokenize("\n"))
       .setHeader("x-bip-message-id", simple("${header.x-bip-message-id}-${exchangeProperty.CamelSplitIndex}"))
       .to("https://receiver.example/lines").id("post-line")
   .end()
   ```

   Prefer a business key from the record over the split index when the input
   can change between replays.
2. Publish each part with its own id, for example as a Kafka record to the
   integration's own input, and send it from there.

A part sent through a new exchange, for example with a `ProducerTemplate`, is
not a safe form on its own. Sent straight to the receiver's endpoint, the send
is not tracked: it has no step key, the receiver gets no `Idempotency-Key`, and
a replay sends it again. Sent to a route that contains the tracked send, such as a
`direct:` route, it is tracked under the new exchange's `x-bip-message-id`.
Without its own id, each send then gets a new random id, so a replay sends it
again too. The input's journal row does not list the part's send. The part is
journaled only when it goes to a route selected for delivery audit or input
inspection, as its own row that cannot be replayed. Delivery audit refuses a
request with its own `x-bip-message-id`, so a replay sends such a part again;
input inspection keeps the part's own id.

If an aggregation strategy returns a part's message, restore the input's id
after the split so that the input is journaled under its own id. After a route
moves to per-part ids, its keys change: a replay of a message recorded before
the change sends its first part once more. 0.4 gives each part and iteration
its own step key. The [split step identity advisory](docs/split-step-identity.md)
has the exact conditions and the cases the ERROR line does not catch.

## Key-Value Store

0.3.6 adds [`KeyValueStore`](docs/key-value-store.md) for small
integration-scoped JSON cursors and markers in the `bip_key_value` table, which
the database operator provisions in the Message Store. It keeps the full
maintenance baseline and adds no leases or separate store service.
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
when a separate service consumes `bip.message.status` and writes the Postgres
read model. Without such a service, the Kafka sink only publishes status
events; nothing records them to Postgres.

For manual replay with this adapter, use the `postgres` sink. The `kafka`
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

### Schema version startup gate

When `basis.message-store.sink=postgres` and a JDBC
datasource is available, the adapter reads
`select max(contract_major) from bip_contract` at startup:

- table missing (or empty): a loud warning is logged and startup continues,
  assuming store schema version (contract major) 1.
- `max(contract_major) != 1`: startup fails with an error naming the found and
  expected major, instead of silently writing a mismatched schema shape.

## Configuration

Adapter defaults:

```properties
basis.message-store.sink=log
basis.message-store.status-topic=bip.message.status
basis.message-store.step-outcome-store=noop
basis.message-store.statement-timeout=10s
basis.message-store.lock-timeout=5s
camel.component.kafka.auto-commit-enable=false
camel.component.kafka.allow-manual-commit=true
camel.component.kafka.auto-offset-reset=earliest
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
| `basis.message-store.key-value.sweep-interval-seconds` | integer seconds, `1..3600` | `3600` | Once the Key-Value Store is used, sweep this integration’s expired keys in bounded batches. |
| `basis.message-store.sink` | `log`, `postgres`, `kafka` | `log` | Selects where terminal message outcomes go. |
| `basis.message-store.step-outcome-store` | `noop`, `postgres` | `noop` | Selects whether delivered-step checks are durable. |
| `basis.message-store.statement-timeout` | duration, at least `1ms` | `10s` | From 0.3.7: bounds each statement of a Postgres sink or step outcome store call. See [Message Store call timeouts](docs/configuration.md#message-store-call-timeouts). |
| `basis.message-store.lock-timeout` | duration, at least `1ms` and at most the statement timeout | `5s` | From 0.3.7: bounds each such statement's wait for a lock. |
| `basis.message-store.step-ids.emergency-allow-generated` | `true`, `false` | `false` | Emergency escape hatch: start even though a tracked send has no explicit id, with a warning for each. See [step tracking](#step-tracking). |
| `basis.message-store.payload.mode` | `postgres-inline`, `metadata-only` | `postgres-inline` | Selects whether safely capturable payloads are stored inline for replay. |
| `basis.message-store.status-topic` | Kafka topic name | `bip.message.status` | Used only when `basis.message-store.sink=kafka`. |
| `basis.message-store.dlq-uri` | Camel endpoint URI | `kafka:bip.message.dlq` | Target used by the shared dead letter channel. Use a fixed URI of a durable, non-transactional endpoint that fails when it cannot store the record: a send that returns without an error counts as parked. A custom parking route must not stop, filter or drop the record. |
| `basis.message-store.payload.max-bytes` | integer bytes, `>= 0` | `1048576` | Payloads above this cap become `metadata_only`. |
| `basis.message-store.payload.retention-days` | integer days, `>= 0` | `30` | Used to set `payload_expires_at`. Cleanup is a separate future job. |
| `basis.message-store.replay.enabled` | `true`, `false` | `true` | If false, payloads are metadata-only and replay is disabled by policy. |
| `basis.message-store.replay.input-topic-pattern` | Java format string with one `%s`, or fixed topic | `bip.%s.in` | Used by `MessageReplayService` to choose the replay target topic. |
| `camel.component.kafka.auto-commit-enable` | `true`, `false` | `false` | Must stay false for adapter-managed commit. |
| `camel.component.kafka.allow-manual-commit` | `true`, `false` | `true` | Must stay true for adapter-managed commit. |
| `camel.component.kafka.auto-offset-reset` | `earliest`, `latest`, `none` | `earliest` | Must stay `earliest` for every managed Kafka input, on the component, the endpoint (`autoOffsetReset`) and `additionalProperties.auto.offset.reset`; another value is logged as a warning at startup. A new consumer group starts at the beginning of each partition's retained records. Give every Kafka input a stable `groupId`. |

Direct Postgres production mode:

The adapter JAR defaults to `log` and `noop` so adding the dependency does not
force every local/test integration to have Postgres available. Production
Camel Quarkus integrations should get the production defaults from their
deployment configuration, not from the adapter library itself.

```properties
basis.message-store.sink=postgres
basis.message-store.step-outcome-store=postgres
basis.message-store.payload.mode=postgres-inline
basis.message-store.replay.enabled=true
basis.message-store.replay.input-topic-pattern=bip.%s.in
```

These settings belong in every production integration's deployment
configuration unless an integration has a deliberate reason to opt out.

Tests and local smoke routes can override `basis.message-store.dlq-uri` to a
non-Kafka endpoint, for example `direct:test-dlq`, when no Kafka broker is
available.

## Status

- Direct Postgres mode is the production path. The deployment sets the
  production defaults and the integration's build pins the adapter version;
  integrations do not carry their own resend or DLQ plumbing.
- The .NET adapter (`Basis.IntegrationPlatform.MessageStore` on NuGet.org)
  implements the same message-store tables, topics and headers.
- The message-store schema is provisioned by the database operator, outside
  the integration; the SQL files shipped in this JAR are local/test resources
  only.
- The Kafka status-event sink is not the production path: nothing records
  `bip.message.status` events to Postgres until a separate service consumes
  them.

## Future Direction

Possible later work, not part of 0.3.x and not a rewrite of business routes:

- A separate message-store service that captures messages at the Kafka
  boundary (real `RECEIVED`/in-flight visibility) and becomes the single
  journal writer.
- Move from adapter-direct Postgres persistence to status/lifecycle events
  once that service owns persistence. The existing `MessageStoreSink`
  boundary is the switch point — migration is a per-integration sink flip.
- Split `steps_json` into dedicated relational tables only if operator query
  needs justify that change.
- Consider external payload storage for large/sensitive payloads instead of
  inline Postgres bytes.

## License

Apache-2.0. Issues and pull requests are welcome and handled best-effort.

## Authenticated Kafka broker

Version 0.3.3 adds one operator-owned security configuration for Camel Kafka
consumption/DLQ and the direct Kafka status/replay producers. It retains the
0.3 runtime model; the separate 0.4 development line is not required.

Set `basis.message-store.kafka.security-protocol=SASL_SSL`,
`basis.message-store.kafka.sasl-mechanism=SCRAM-SHA-512` and the secret
`basis.message-store.kafka.sasl-jaas-config`. The deployment supplies the broker
CA through `basis.message-store.kafka.ssl-truststore-location`; select `PEM`
with `basis.message-store.kafka.ssl-truststore-type` for a PEM certificate.
Hostname verification stays enabled. Missing SASL credentials fail startup.
Default plaintext behavior remains for environments that have not opted in.

The adapter's PostgreSQL datasource belongs only to the adapter's Message Store.
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

Existing message rows may still advertise replayability from old capture
metadata; the server-side check is authoritative and returns a refusal. The
patch does not rewrite historical rows or remove records already published to
Kafka. Safe input replay still retains message identity, increments the manual
attempt and preserves the existing redo-step behavior. Request acceptance is
not proof of delivery or of successful offset commit.

The [application database continuity fixture](integration-tests/application-data-smoke/README.md)
checks a named business datasource alongside a legacy Message Store with no KV
table. Its baseline and candidate results must be recorded separately; upgrading
the adapter does not move application state.

## Optional activity metrics

0.3.6 adds explicitly selected Kafka admission and
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
