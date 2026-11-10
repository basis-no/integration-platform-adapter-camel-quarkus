# Troubleshooting

## No Message Row Appears

- Confirm the route is a selected business-message boundary. `auto` selects
  Kafka consumer routes only.
- Check whether `basis.messageStore.recordMessage=false` was set.
- Confirm the effective sink is `postgres`, not `log` or the Kafka status sink.
- Check for a test profile that disables persistence.
- Verify the message-store schema and datasource binding.

## Message Completes but Kafka Offset Does Not Advance

- Check sink, DLQ, and step-store errors before looking at the business route.
- A message recorded `FAILED` at a Kafka boundary was not parked, for example
  because the DLQ publish failed or a step outcome could not be recorded during
  a Message Store outage. Its offset is not committed and the consumer
  reads it again until the DLQ accepts it; the ERROR log names the message.
- An error that says a statement was cancelled because of a statement or lock
  timeout or "due to user request" (the JDBC query timeout, used inside an
  application transaction), or a socket read timeout, means a Message Store
  statement took longer than `basis.message-store.statement-timeout` or waited
  longer than `basis.message-store.lock-timeout`: check the database's health, its locks
  (`pg_locks`, `pg_stat_activity`) and its network before raising either value.
- An error that says a Message Store transaction "committed locally, but a
  synchronous standby did not confirm it" means a synchronous commit's wait was
  cancelled. The record is read again and its outcome written again; check the
  synchronous standby and the database's replication status
  (`pg_stat_replication`).
- An error that says a Message Store transaction "committed, but the commit
  reported a warning" means the commit succeeded but PostgreSQL reported a
  warning with it, which the adapter cannot tell apart from an unconfirmed
  commit. The record is read again, and fails again, until the warning's source
  is removed: for example a deferred constraint trigger with `RAISE WARNING`
  on a `bip_*` table. The error names the warning.
- Confirm `allow-manual-commit=true`, `auto-commit-enable=false` and
  `break-on-first-error=true`.
- Check whether the guarded `no-commit` test profile is active.
- A running no-commit consumer advances its in-memory position; inspect broker
  group offsets rather than expecting an immediate duplicate delivery.

## Route Fails at Startup

Common fail-fast causes include:

- a Kafka boundary route whose consumer does not use
  `auto.offset.reset=earliest`: remove the `autoOffsetReset` or
  `additionalProperties.auto.offset.reset` override from the endpoint and from
  `camel.component.kafka.auto-offset-reset`, or set it to `earliest`;
- unsupported sink, payload, failure-mode, or test-profile value;
- non-default test profile without explicit opt-in;
- Postgres modes without a resolvable datasource;
- tiered mode without a payload bucket/S3 client;
- conflicting step classification ids;
- normalization route missing `bip-ingress-normalization`;
- normalization route also selected as a lifecycle boundary;
- normalization without the durable Postgres step store;
- unsafe polling with missing or multiple declared replicas;
- store schema version mismatch in `bip_contract`;
- a Message Store without a table or column this adapter writes, for example
  one provisioned for an earlier adapter version. The error names the missing
  columns and the statements that add them;
- a Message Store statement or lock timeout under 1 ms, or a lock timeout
  longer than the statement timeout;
- a setting of adapter 0.3.x that this version does not read
  (`basis.delivery-audit.routes`, `basis.input-audit.routes`); the error names
  each one and what to use instead. See
  [Settings Removed Since 0.3.x](configuration.md#settings-removed-since-03x).

Treat these as configuration errors. Do not bypass them in business routes.

Adapter 0.3.x and 0.4.0-rc1 let Camel log, at route start, that
`StepTrackingInterceptStrategy` "does not return an AsyncProcessor instance",
once for each processor that the adapter does not track and that is not
asynchronous, such as `validate`. The
warning is harmless: Camel bridges the processor itself. Fixed for 0.4.0.

## Ambiguous Step Warning

The adapter cannot prove whether the endpoint is a safe read or a durable write.
Give the send a stable node id and classify that exact id. Do not suppress the
warning by guessing from the id text.

## A Resend Skips a Step That Failed

On adapter 0.3.x, a failed send in a `direct:` sub-route is recorded
`DELIVERED` when the sub-route's dead-letter channel parks the message, so a
resend skips it and the receiver never gets the message. Resend with the step
in `redoSteps`; see
[Sends in a Sub-Route](steps-and-idempotency.md#sends-in-a-sub-route). This is
fixed for 0.4.0. A message parked before the upgrade still needs the redo.

## Replay Is Disabled

Inspect `payload_storage`, `replayable`, `replay_disabled_reason`, expiry, and
deletion metadata in `bip_message_log`. Typical causes are metadata-only mode,
payload above the configured limit, expired/deleted bytes, unsupported body
type, or the HTTP audit-only profile.

## S3 Replay Fails

Verify bucket/prefix ownership, object existence, expiry, size, SHA-256,
workload identity, endpoint reachability, and encryption permissions. The
adapter intentionally fails closed when claim-check metadata does not match.

## Polling Works with One Replica but Not Several

Uncoordinated polling is rejected with more than one replica. Use `master:`
with the Kubernetes cluster service and leader-election RBAC, use a broker
queue with competing consumers, or keep exactly one replica. Do not hardcode
the deployment count in application properties; the deployment sets it.

## Replayed Route Has a Missing Response Body

The skipped write likely produces output required downstream. Classify it as
`OUTPUT_SIDE_EFFECT` so the adapter checkpoints and restores the response, or
explicitly redo it when the receiver contract makes repetition safe.

## Kafka Sink Produces Events but the Read Model Is Empty

The Kafka sink only publishes terminal events to `bip.message.status`. It does
not write Postgres. Deploy a separate lifecycle consumer that persists those
events, or use the direct Postgres sink.
