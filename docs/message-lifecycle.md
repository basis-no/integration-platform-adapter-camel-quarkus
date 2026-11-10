# Message Lifecycle

## Processing Boundary

The message store represents business messages, not every Camel exchange.
`basis.message-store.boundary-routes=auto` selects Kafka consumer routes.
Internal `direct:` routes therefore do not create extra message attempts.

At a Kafka boundary the lifecycle is:

```text
Kafka record
  -> establish envelope and capture replay payload
  -> run Camel business route and automatic redeliveries
  -> record terminal message attempt and durable step outcomes
  -> publish DLQ record when retries are exhausted
  -> commit consumed Kafka offset only after required durable work succeeds
```

A persistence failure or a failed DLQ publish prevents the managed Kafka commit
and leaves the exchange failed. With `camel.component.kafka.break-on-first-error=true`,
the adapter's default, the consumer reads the record again before the
partition's later records. Kafka can also redeliver a record after a rebalance,
which is why side effects still require stable idempotency.

### Holding Records in Memory

The commit follows the input exchange: once it completes, the adapter records
its outcome and commits its offset. A Camel processor that keeps the record
after that, and releases it later, holds it only in memory: Camel's
`resequence` (batch and stream) and an `aggregate` on its default in-memory
repository. A restart before the record is released loses it; it is neither
delivered nor read again, because its offset is committed
(`InMemoryHoldRestartTest` shows it for all three).

A Kafka boundary route that contains such a processor therefore refuses to
start, naming the route and node. The check covers the route, its
circuit-breaker fallbacks, and the `direct:` routes it sends to with a fixed URI
(a `to`, or a `toD` or `enrich` without `${...}`; property placeholders such as
`direct:{{target}}` are resolved), transitively. It does not follow dynamic
sends, and it does not apply when an application attaches
`MessageStoreRoutePolicy` to a route itself.

To aggregate on a Kafka boundary, give the `aggregate` a persistent
`aggregationRepository`, which keeps the held records across a restart. Only
Camel's `MemoryAggregationRepository` (also the default when none is set) is
recognised as in-memory; whether any other repository is durable is the
application's responsibility. A repository reference is resolved as Camel
resolves it (property placeholders first, then `#name`, `#bean:`, `#type:` or a
plain registry name; `#class:` by its class, without creating it). One that
cannot be resolved when the route is created is refused, since it cannot be
shown not to be in memory. Camel's `resequence` has no such repository; a durable
resequencer is planned for 0.4.0
([#36](https://github.com/basis-no/integration-platform-adapter-camel-quarkus/issues/36))
and is not available yet.

## Status and Attempt Semantics

The Camel adapter records one terminal outcome per message attempt:

- `COMPLETED`: route processing and required outcome recording succeeded.
- `DLQ`: retries were exhausted and the DLQ endpoint accepted the record.
- `FAILED`: terminal failure was recorded without a Kafka DLQ, as used by the
  explicit non-Kafka audit-only preview. At a Kafka boundary, `FAILED` means the
  failure was not parked, for example because the DLQ publish failed or a step
  outcome could not be recorded; the offset is not committed and the record is
  read again.
- `RECEIVED`: reserved for a future capture layer; the Camel terminal flow
  does not emit it.

Camel automatic redeliveries remain within the same message attempt. They are
recorded through `auto_redelivered`, `auto_redelivery_count`, and
`auto_redelivery_max`. Manual replay increments the message attempt.

## Envelope

The adapter preserves or supplies standard `x-bip-*` headers such as message
id, correlation id, content type, source/target system, replay flag, attempt,
redo steps, and step idempotency key. The same `x-bip-message-id` is retained
across manual replay so durable step outcomes remain addressable.

## Persistence Modes

`basis.message-store.sink` selects terminal outcome handling:

- `postgres` writes the canonical `bip_message_log` and
  `bip_message_attempt` read model directly. This is the production mode.
- `kafka` emits terminal lifecycle events to `bip.message.status`. A separate
  lifecycle consumer must persist those events; the adapter does not provide
  that central service.
- `log` writes outcomes to application logs for local diagnostics.

`basis.message-store.step-outcome-store=postgres` separately enables durable
delivered-step checks in `bip_step_outcome`. The sink and step store solve
different problems and production direct-Postgres mode uses both.

## Read Model

- `bip_message_log`: current searchable state for one message.
- `bip_message_attempt`: terminal history for each message attempt, including
  automatic redelivery diagnostics and `steps_json`.
- `bip_step_outcome`: durable delivered-side-effect ledger and optional output
  checkpoint.
- `bip_message_replay_audit`: operator replay request and publication audit.
- `bip_contract`: store schema version (`contract_major`) checked at adapter
  startup, together with the tables and columns the adapter writes.

In production the database operator provisions the schema outside
integration pods. SQL resources in the adapter JAR exist for local tests, not
application-owned migrations.

## Explicit Suppression

A selected boundary may discover that an exchange is technical rather than a
business message. Before any external endpoint send, set:

```yaml
- setProperty:
    name: "basis.messageStore.recordMessage"
    constant: false
```

No message, attempt, status event, or step outcome is then recorded. A Kafka
record is still committed after successful suppressed processing, or once its
failure was parked; a suppressed record that failed without being parked is not
committed. The adapter does not infer suppression from a null or empty body
because an empty Kafka payload can be a legitimate business message.
