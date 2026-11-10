# Retry, DLQ, and Replay

## Automatic Retry

The global route configuration applies unless a route selects a variant:

| Configuration | Redeliveries | Delays | Total wait | Terminal behavior |
| --- | ---: | --- | ---: | --- |
| default | 3 | 1, 2, 4 seconds | 7 seconds | configured failure mode |
| `ip-slow-backend` | 6 | 5, 10, 20, 20, 20, 20 seconds | 95 seconds | configured failure mode |
| `ip-no-retry` | 0 | none | none | configured failure mode |
| `bip-ingress-normalization` | 3 | 1, 2, 4 seconds | 7 seconds | propagate to source |

Retries use exponential backoff; `ip-slow-backend` caps each delay at 20
seconds. Automatic redelivery does not increment the message attempt; it adds
diagnostics to that attempt.

On a Kafka input the waits run on the consumer thread, between attempts that
take their own time. `max.poll.interval.ms` (300 seconds by default) bounds the
time between two polls, and the consumer processes every record of a poll
before it polls again (up to 500 records, Camel's default `maxPollRecords`), so
the budget covers all records of one poll, not one record. If they take longer,
Kafka removes the consumer from its group and the uncommitted records are
delivered again. With `ip-slow-backend`, one record that exhausts its retries
waits 95 seconds, so three such records in one poll already take most of the
interval. For a route on `ip-slow-backend`, set `maxPollRecords=1` on the
Kafka input, or raise `maxPollIntervalMs` on it to cover the records of a poll.
With one record per poll, about 200 seconds remain for its seven attempts:
bound each receiver call well inside that with `responseTimeout` and
`connectTimeout` (see the table below for the forms Camel applies), and turn
the HTTP client's own retry off, as described next, since it can double the
requests in each attempt.

The count is Camel's redeliveries, not the requests a receiver saw. The HTTP
client inside Camel's `http` component (Apache HttpClient 5.5 and its default
retry strategy) also retries by itself, within one Camel attempt: it repeats a
request answered with 503 or 429 once, POST included. Before that retry it waits
for the receiver's `Retry-After`, given in seconds or as a date, with no upper
limit; without `Retry-After` it waits one second. The wait runs on the Kafka
consumer thread, so it counts against `max.poll.interval.ms` (300 seconds by
default) together with the route's redeliveries and receiver timeouts. If
processing takes longer, Kafka removes the consumer from its group and the
uncommitted record is delivered again.

Two 503 answers and then success can therefore record one redelivery for three
requests; with the default policy, a receiver that keeps answering 503 can get
eight requests before the message is parked. Set
`camel.component.http.automatic-retries-disabled=true` to leave every retry to
the adapter's retry policy. Set it for receivers that send a long `Retry-After`,
and for routes on the `ip-slow-backend` configuration, whose own redeliveries
already use about a third of the poll interval.

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

The adapter logs a warning for each option in the table when it creates a route
with it in a `to`, `toD`, `enrich` or `wireTap` URI, once per route, step and
option. The warning names the option, the step and the replacement, never the
URI or a value. A URI in a `recipientList` or
`routingSlip`, or one whose scheme is only known at send time, is not checked.

A timeout does not mean the receiver did nothing. A receiver can apply the
effect after the client gave up, and the message is then retried and parked
although the effect happened. The step is recorded `FAILED`, so a replay sends
it again, with the same `Idempotency-Key` (`<message-id>:<step-id>`) as every
earlier try. Only a receiver that deduplicates on that key avoids a second
effect. Republishing the parked record under a new message id changes the key,
so even such a receiver applies it again.

## Failure and Replay per Input Type

What happens to a message whose route failed after its retries, and where a
replay starts, follows the input type. One application may combine them; no
global setting is needed.

| Input | After the retries | Source | Recorded | Replay |
| --- | --- | --- | --- | --- |
| Kafka boundary | parked in the DLQ with the consumed input | offset committed after the DLQ accepted it; not committed if the outcome or the parking is not durable | `DLQ` | from the Kafka input topic |
| HTTP audit-only boundary | handed back to the caller (HTTP error) | none: the caller has the answer | `FAILED`, metadata-only | not replayable |
| HTTP intake that publishes to Kafka | handed back to the caller | none | at the Kafka boundary | from the Kafka input topic |
| File, FTP, SFTP normalized to Kafka | handed back to the file consumer | the file is not moved and is read again on a later poll | at the Kafka boundary, once published | from the Kafka input topic, not the file |
| Timer or database poll normalized to Kafka | handed back to the poll | depends on the poll: the next run finds the item again only if it is still selected | at the Kafka boundary, once published | from the Kafka input topic |
| JMS queue normalized to Kafka | handed back to the JMS consumer | with `CLIENT_ACKNOWLEDGE`, not acknowledged, so the broker delivers it again | at the Kafka boundary, once published | from the Kafka input topic, not the queue |

A normalized input is recorded at the Kafka boundary that consumes the
canonical topic, so its replay starts there and never reads the file, queue or
poll again. A synchronous HTTP request has no replay: a replay could not answer
the original caller. Accept the request and publish it to Kafka to make the
downstream work replayable. See [Ingress and Scaling](ingress-and-scaling.md)
for the configuration of each input.

`basis.message-store.failure-mode` can stay unset (default `dlq`). Its other
value, `record-failed`, turns DLQ parking off for the whole application;
startup refuses it when the application has a Kafka boundary, since it would
remove that boundary's recovery path.

## Terminal Kafka Failure

After a Kafka boundary's retries are exhausted, the adapter sends the original
captured payload to `basis.message-store.dlq-uri`, preserves the envelope, adds
`x-bip-error` and `x-bip-dlq-at`, records `DLQ`, and only then commits the input
offset. A replay's `x-bip-redo-steps` is not carried; see [Redo Steps](#redo-steps).

The message counts as parked only after the DLQ endpoint accepted the record; a
Kafka producer with Camel's default `requestRequiredAcks=all` returns once the
broker acknowledged it. The publish runs in the adapter's
`bip-confirmed-dead-letter` route. If it fails, the adapter records `FAILED`,
does not commit the offset and leaves the exchange failed. With the adapter's
default `camel.component.kafka.break-on-first-error=true`, the consumer then
reads the record again before the partition's later records, so the partition
waits until the DLQ accepts the record. Each attempt logs at ERROR. A record
that the DLQ endpoint can never accept, for example one above the DLQ topic's
size limit, keeps its partition waiting until an operator fixes the DLQ or
moves the consumer group past it.

### Reading a record again

A Kafka record whose outcome could not be made durable, because the outcome
write, a step outcome check or write, or the DLQ publish failed, is not
committed and is read again. The first time, it is read again at once. If the
same record comes back with its outcome still not durable, the adapter waits
before handing it back: 1 second, doubling on each further read, at most 30
seconds. The wait is also kept to half the consumer's effective
`max.poll.interval.ms` (an `additionalProperties` value overrides the named
option) less the time since the records of the poll that returned it began to
be processed, including the records before it, so that the consumer polls again
within its interval. This relies on `break-on-first-error=true`, the adapter's
default, under which a record that is read again ends its poll; without it
there is no wait. A committed outcome on the partition, or a later record,
starts again at once. The consumer waits for it, also when the route completes
on another thread, so the consumer's other partitions wait too, as they do
after any record that is read again. The wait ends early when the application
stops. Before, the record was read again at once
for as long as the failure lasted, several times a second, and each read ran
the route again, including sends that the step store did not skip and another
DLQ record for a message that was parked. A record whose topic, partition and
offset the adapter did not see at the start is read again at once, as before.

`basis.message-store.dlq-uri` must name one fixed endpoint that reports a
failed publish as an error: a `kafka:` endpoint whose producer waits for the
broker and is not transactional, or the application's own `direct:` route. A
Kafka DLQ receives one record per message, also when the parked body is a
collection. The adapter counts a message parked when a `direct:` route
completes without an error, also when it ends with `stop()`; give that route
`errorHandler(noErrorHandler())` so that its failures reach the adapter.
Startup fails for an expression such as `${header.name}`, which can evaluate
to no endpoint at all, for Kafka with `acks=0` or a transactional producer
(`transacted`, `transactionalId` or `transactional.id`), whose transaction
commits only after the input offset, for `direct:` with
`failIfNoConsumers=false`, and for any other endpoint.

Only a managed Kafka input is parked: an exchange that carries the consumed
record's commit handle, which the route policy keeps from the Kafka manual
commit header. That includes the copies Camel makes of the input, for a split,
multicast, recipient list, enrich or wire tap. The adapter's dead letter channel
applies to every route, but an exchange without the handle, such as a request
that a route sends to a `direct:` route with a `ProducerTemplate`, or an input
from a timer or poller, fails back to its caller with its own exception:
no DLQ record is written for it, and no ERROR says it is parked. The caller owns
its recovery, for example by letting the failure fail the Kafka input, which is
then retried and parked. A `direct:` route that runs on the input's own
exchange, as a plain `to("direct:…")` does, is part of the input and parks it.

An application's own `onException` changes this. If it leaves a failure
unhandled (`handled(false)`), the adapter does not park it: the message is
recorded `FAILED`, not committed, and read again. If it marks the failure
handled, the application has taken it over: the message is recorded
`COMPLETED` and committed, and nothing is parked.

Such a handler, in a route, a route builder or a route configuration, sees and
sends the message as the route left it, as a `doCatch` does, also with
`continued(true)` for the rest of the route: the body the route left, its own
headers, and neither `x-bip-error` nor `x-bip-dlq-at`. The adapter prepares the
parked record, and logs the parking at ERROR, only in its dead-letter route,
when the dead letter channel actually parks the message. To work with the
consumed input in a handler, use Camel's `useOriginalMessage()` on the
`onException` and `${exception.message}` for the error.

Camel skips the dead letter channel's preparation and the adapter's
dead-letter route for an exchange that its route stopped
(`exchange.setRouteStop(true)`) before it failed. The adapter then records
`FAILED` and does not commit the offset, and the consumer reads the record
again, so it keeps its partition waiting like a record the DLQ cannot accept.

A message whose attempt could not check or record a step outcome is not parked
either: a parked copy would repeat an input that is consumed again. It is
recorded `FAILED` and not committed; see
[Steps and Idempotency](steps-and-idempotency.md#when-a-step-outcome-cannot-be-recorded).

The historical Kafka DLQ record remains after recovery. Postgres is
authoritative for current lifecycle state, so a later successful replay changes
the message to `COMPLETED` while retaining both attempts and replay audit.

Any other input is not parked: the dead letter channel parks only a managed
Kafka input and hands every other failure back to its source (see
[Failure and Replay per Input Type](#failure-and-replay-per-input-type)).

## Manual Replay

The adapter exposes:

```http
POST /admin/messages/{messageId}/resend
```

The replay service validates the stored inline payload or S3 claim check,
writes a `bip_message_replay_audit` row, then publishes to the configured input
topic. It retains the message id and envelope, sets `x-bip-replay=true`, and
sets `x-bip-attempt` to the previous attempt plus one.

An integration replays only its own messages, to the input that recorded them.
Before it writes the audit row or publishes anything, the replay service
refuses a message:

- whose stored integration is not this integration (`app.name`);
- whose last attempt was recorded by a route that is not a Kafka consumer of
  exactly this integration's replay topic in the running application: a route
  that was removed or renamed, consumes another topic, a list of topics or a
  topic pattern, or is not a Kafka route;
- whose last attempt is missing, or holds another payload than the message.

The endpoint answers `409 Conflict` with the reason. A message recorded by a
route that has since changed needs review; send its payload again as a new
message once it is safe to process.

The caller, typically an operator's tool, supplies operator identity and reason
through the `x-dashboard-user` and `x-dashboard-reason` headers or the JSON
request body. The default target pattern is `bip.%s.in`, where `%s` is the
stored integration name.

Replay is a new Kafka record appended to a partition; it does not move the
original record back to its historical offset. Kafka ordering is guaranteed
only within a partition, and bulk replay needs a separately defined ordering
policy.

### Replay order

A replay record goes to the end of the integration's replay topic, never back
to the original offset. Its key is the message's stored correlation id: the
`x-bip-correlation-id` the exchange carried when its outcome was recorded,
or else the one the consumed record had. Without either, the replay record has
no key, and Kafka places it on any partition; there is no fallback to the
original record's key or to the message id. The adapter publishes the key as
UTF-8 text with the Kafka client's default partitioner. The replay therefore
lands on the original record's partition when the input's producer also
published that key as the same UTF-8 bytes, with the default partitioner, no
explicit partition and the same partition count. That is not guaranteed when:

- the input's producer keyed its records by something else than the correlation
  id, serialized the key differently, chose the partition itself, or used
  another partitioner;
- the route changed `x-bip-correlation-id`, or the record had none;
- the topic's partition count changed.

Kafka keeps order only within a partition, so even on the same partition a
replay comes after every record that was appended in the meantime: a newer
message for the same entity may already have been processed. A replay of
several messages has no overall order. When you resend several messages:

- resend per key, oldest first, and wait until each has been processed (its
  outcome recorded), not only accepted, before the next;
- do not resend a failure that a newer message for the same key has already
  superseded: replaying it would restore stale data;
- keep `x-bip-correlation-id` equal to the record key when order per entity
  matters, so that the replay key and the original key agree.

The .NET adapter keys its replay records by the correlation id too, but falls
back to the original record's key and then to the message id when there is no
correlation id, and its Kafka client places keys with a different default
partitioner than Java clients.

## Redo Steps

Ordinary replay skips side effects already recorded `DELIVERED`. An operator
may intentionally re-execute selected steps by supplying `redoSteps`. The
adapter publishes the JSON array in `x-bip-redo-steps` and stores it in replay
audit. A listed step id or full idempotency key bypasses the delivered-step
skip.

Redo deliberately repeats an external effect. It is safe only when the
receiver honors the stable idempotency key or repetition is acceptable.

Redo needs recorded step outcomes. With
`basis.message-store.step-outcome-store=noop`, the default, nothing is recorded
as delivered and every step runs on every replay, so a request whose
`redoSteps` names a step is refused with `409 Conflict` before the replay audit
row is written or anything is published. Resend it without `redoSteps`. A list
of blank entries counts as no redo. With `postgres`, redo works as described
above. The rule asks the step outcome store in use, so an application's own
`StepOutcomeStore` bean that replaces the adapter's decides through
`recordsDeliveredSteps()`, which is `true` unless it overrides it. The same rule
applies in the .NET adapter.

The list stays inside the integration. When an exchange enters a route, the
adapter reads `x-bip-redo-steps`, keeps the list for the attempt's step tracking
and removes the header: an input that the Message Store records before the
route runs, and every other route through a route policy the adapter adds. The
route's sends therefore do not carry it, whether through `to`, `toD`, `enrich`,
`wireTap`, `recipientList` or `routingSlip`; otherwise a receiver would get the
list, and a downstream integration that consumed a published record with the
same message id would send its own steps with the same ids again. A handler
that restores the original message, for example with `useOriginalMessage()`,
gets the header back; the next `to` or `toD` and DLQ parking remove it again.
`x-bip-replay` and `x-bip-attempt` are still forwarded; nothing in the adapter
acts on `x-bip-replay` at a Kafka input. The warning for a malformed list names
the header only, not its value.

## Schema Version Startup Gate

With the Postgres sink, startup reads the highest store schema version
(`contract_major`) from `bip_contract`. A version other than 1 fails startup. A
missing table or empty row is treated as version 1 with a prominent warning;
other database errors are not treated as a missing schema.

With the Postgres sink or the Postgres step store, startup also checks, with
one catalog query, that the store has every table and column this adapter
writes, as defined by the schema files it ships in `db/message-store/`:
`bip_message_log` and `bip_message_attempt` with the Postgres sink,
`bip_message_replay_audit` when replay is enabled as well, and
`bip_step_outcome` with the Postgres step store. A store provisioned for an
earlier adapter version, for example one without `payload_ref` or the
`checkpoint_*` columns, fails startup. The error names each missing table and
column and the `alter table … add column if not exists …` statements that add
them; run them, or the schema file for a missing table, as the database
operator before starting. The check does not compare types, defaults or check
constraints: a store whose `payload_storage` check does not allow `s3_object`
still fails object-backed writes in `tiered` mode.
