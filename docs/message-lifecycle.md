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

A persistence or required DLQ failure prevents the managed Kafka commit. Kafka
can then redeliver the record, which is why side effects still require stable
idempotency.

## Status and Attempt Semantics

The Camel adapter records one terminal outcome per platform attempt:

- `COMPLETED`: route processing and required outcome recording succeeded.
- `DLQ`: retries were exhausted and the record was parked successfully.
- `FAILED`: terminal failure was recorded without a Kafka DLQ, as used by the
  explicit non-Kafka audit-only preview.
- `RECEIVED`: reserved for a future platform capture layer; the v1 Camel
  terminal flow does not emit it.

Camel automatic redeliveries remain within the same platform attempt. They are
recorded through `auto_redelivered`, `auto_redelivery_count`, and
`auto_redelivery_max`. Manual replay increments the platform attempt.

## Envelope

The adapter preserves or supplies standard `x-bip-*` headers such as message
id, correlation id, content type, source/target system, replay flag, attempt,
redo steps, and step idempotency key. The same `x-bip-message-id` is retained
across manual replay so durable step outcomes remain addressable.

## Persistence Modes

`basis.message-store.sink` selects terminal outcome handling:

- `postgres` writes the canonical `bip_message_log` and
  `bip_message_attempt` read model directly. This is the v1 production mode.
- `kafka` emits terminal lifecycle events to `bip.message.status`. A separate
  lifecycle consumer must persist those events; the adapter does not provide
  that central service.
- `log` writes outcomes to application logs for local diagnostics.

`basis.message-store.step-outcome-store=postgres` separately enables durable
delivered-step checks in `bip_step_outcome`. The sink and step store solve
different problems and production direct-Postgres mode uses both.

## Dashboard Read Model

- `bip_message_log`: current searchable state for one message.
- `bip_message_attempt`: terminal history for each platform attempt, including
  automatic redelivery diagnostics and `steps_json`.
- `bip_step_outcome`: durable delivered-side-effect ledger and optional output
  checkpoint.
- `bip_message_replay_audit`: operator replay request and publication audit.
- `bip_contract`: schema contract major checked at adapter startup.

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
record is still committed after successful suppressed processing. The adapter
does not infer suppression from a null or empty body because an empty Kafka
payload can be a legitimate business message.
