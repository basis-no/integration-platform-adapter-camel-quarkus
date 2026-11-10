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
and leaves the exchange failed. A Message Store call that reaches its statement,
lock or network timeout is such a failure (see
[Message Store call timeouts](configuration.md#message-store-call-timeouts)). With `camel.component.kafka.break-on-first-error=true`,
the adapter's default, the consumer reads the record again before the
partition's later records. Kafka can also redeliver a record after a rebalance,
which is why side effects still require stable idempotency.

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
  startup.

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
