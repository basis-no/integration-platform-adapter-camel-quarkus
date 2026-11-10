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
- an id in `basis.message-store.single-active.excluded-routes` that names a
  route the adapter does not run under a lease;
- store schema version mismatch in `bip_contract`;
- a Message Store without a table or column this adapter writes, for example
  one provisioned for an earlier adapter version. The error names the missing
  columns and the statements that add them;
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

## Pod Stays Ready After the Consumer Stopped

The adapter adds no consumer health check. Add Camel's health bridge
(`camel-quarkus-microprofile-health`) and give the readiness probe a timeout of
3–5 seconds; see
[Health and Readiness](getting-started.md#health-and-readiness).

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

A polling route runs on the replica that holds its lease (see
[Single-Active Routes](single-active.md)); on the other replicas it is started
but does no work. The holder is the Key-Value Store key `lease/<routeId>`. A
route listed in `basis.message-store.single-active.excluded-routes` is not
coordinated, so an ingress normalization route on it is rejected with more
than one declared replica: remove it from the list, use `master:` with the
Kubernetes cluster service and leader-election RBAC, use a broker queue with
competing consumers, or keep exactly one replica. Do not hardcode the
deployment count in application properties; the deployment sets it.

## A Polling Route Does Nothing

- Another replica holds `lease/<routeId>`: the route works there. After a crash
  the lease expires within 30 seconds.
- The Key-Value Store is unreachable: the route's work stays stopped on every
  replica (WARN "waits: the Key-Value Store is unreachable") until it is
  reachable again.
- The Message Store check has not passed or failed at startup: no lease is
  taken until it passes.
- The route is an ingress normalization route the replica check accepted
  because of its lease, and the Message Store has no `bip_key_value` table
  (ERROR "stays stopped on every replica: it needs its lease"): provision the
  table, or declare one replica.
- The gauge `integration_single_active_lease{route=…}` shows this replica's
  state: `1` held, `0` waiting, `2` uncoordinated.

## Replayed Route Has a Missing Response Body

The skipped write likely produces output required downstream. Classify it as
`OUTPUT_SIDE_EFFECT` so the adapter checkpoints and restores the response, or
explicitly redo it when the receiver contract makes repetition safe.

## Kafka Sink Produces Events but the Read Model Is Empty

The Kafka sink only publishes terminal events to `bip.message.status`. It does
not write Postgres. Deploy a separate lifecycle consumer that persists those
events, or use the direct Postgres sink.
