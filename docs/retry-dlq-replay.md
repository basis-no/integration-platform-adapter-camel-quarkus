# Retry, DLQ, and Replay

## Automatic Retry

The global route configuration applies unless a route selects a variant:

| Configuration | Redeliveries | Initial delay | Terminal behavior |
| --- | ---: | ---: | --- |
| default | 3 | 1 second | configured failure mode |
| `ip-slow-backend` | 6 | 5 seconds | configured failure mode |
| `ip-no-retry` | 0 | none | configured failure mode |
| `bip-ingress-normalization` | 3 | 1 second | propagate to source |

Retries use exponential backoff. Automatic redelivery does not increment the
message attempt; it adds diagnostics to that attempt.

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
already use much of the poll interval.

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

## Terminal Kafka Failure

`basis.message-store.failure-mode=dlq` is required for Kafka lifecycle
boundaries. After retries are exhausted, the adapter sends the original
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

An application's own `onException` changes this. If it leaves a failure
unhandled (`handled(false)`), the adapter does not park it: the message is
recorded `FAILED`, not committed, and read again. If it marks the failure
handled, the application has taken it over: the message is recorded
`COMPLETED` and committed, and nothing is parked.

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

`record-failed` is reserved for explicit non-Kafka audit-only boundaries. It
records `FAILED` and propagates the exception to the source; using it on a Kafka
boundary is rejected because it would remove the recovery path.

## Manual Replay

The adapter exposes:

```http
POST /admin/messages/{messageId}/resend
```

The replay service validates the stored inline payload or S3 claim check,
writes a `bip_message_replay_audit` row, then publishes to the configured input
topic. It retains the message id and envelope, sets `x-bip-replay=true`, and
sets `x-bip-attempt` to the previous attempt plus one.

The caller, typically an operator's tool, supplies operator identity and reason
through the `x-dashboard-user` and `x-dashboard-reason` headers or the JSON
request body. The default target pattern is `bip.%s.in`, where `%s` is the
stored integration name.

Replay is a new Kafka record appended to a partition; it does not move the
original record back to its historical offset. Kafka ordering is guaranteed
only within a partition, and bulk replay needs a separately defined ordering
policy.

## Redo Steps

Ordinary replay skips side effects already recorded `DELIVERED`. An operator
may intentionally re-execute selected steps by supplying `redoSteps`. The
adapter publishes the JSON array in `x-bip-redo-steps` and stores it in replay
audit. A listed step id or full idempotency key bypasses the delivered-step
skip.

Redo deliberately repeats an external effect. It is safe only when the
receiver honors the stable idempotency key or repetition is acceptable.

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
