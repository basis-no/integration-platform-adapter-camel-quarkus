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
- `basis.message-store.failure-mode=record-failed` in an application with a
  Kafka boundary: remove the setting; an HTTP audit-only boundary needs no
  failure mode of its own;
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
- a polling route whose consumer the adapter cannot gate (neither a
  scheduled-poll consumer nor a timer, Quartz or cron trigger); list it in
  `basis.message-store.single-active.excluded-routes`;
- store schema version mismatch in `bip_contract`;
- a Message Store without a table or column this adapter writes, for example
  one provisioned for an earlier adapter version. The error names the missing
  columns and the statements that add them;
- a Kafka boundary route that holds records in memory after its input
  completes: a `resequence`, or an `aggregate` on Camel's in-memory repository
  (or a repository reference that cannot be resolved), in the route or in a
  `direct:` route it sends to.
  A restart would lose the held records, whose offsets are already committed.
  See [Holding Records in Memory](message-lifecycle.md#holding-records-in-memory);
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

## A Receiver Answering 429 or 503 Gets Fewer or Later Requests

The adapter turns off the HTTP client's own retry and times each redelivery
after a 429 or 503 by the receiver's `Retry-After`, within the Kafka poll
budget. A receiver therefore gets one request per Camel attempt, and
`auto_redelivery_count` matches its requests after the first. A `Retry-After`
too long for the budget is not waited for: the configured delays apply, and
the message is parked when the retries are exhausted. See
[Automatic Retry](retry-dlq-replay.md#automatic-retry).

## Message Id Is Also Journaled by Another Integration

A warning `Message id <id> is also journaled by integration <other>;
integration <this> records attempt <n> under the same id` means two
integrations journal the same `x-bip-message-id` to one Message Store. The
store keeps one summary row per message id and one attempt row per id and
attempt, so the two integrations can replace each other's rows. Typically the
one that wrote last owns the message (the summary is replaced only by an
attempt at least as new as the one it holds): the other one no longer sees it
in its view and cannot replay it, and a send with the same step id is skipped
as already delivered. The usual cause is an upstream integration that publishes
to this integration's input with its own `x-bip-message-id`, which Camel's
Kafka producer forwards by default. Set a new `x-bip-message-id` before
publishing to another integration's input, and carry continuity in
`x-bip-correlation-id`. The warning is logged once per pair of integrations,
then at most every ten minutes with the number of such messages.

With the optional message recovery SQL, or row-level isolation between
integrations, this warning does not appear: the second integration's write is
refused instead (`RECOVERY_SCOPE_MISMATCH` or a row-level security error in
its log), and that refusal is the signal. The warning is temporary: it goes
away when each integration owns its message ids
([#30](https://github.com/basis-no/integration-platform-adapter-camel-quarkus/issues/30)).

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

An object-stored input that fails its check, or cannot be checked, is logged at
ERROR as `Message <id> did not reach its route: <reason>`, and its route does
not run.

A Kafka claim check that can never pass ends the line with `; it can never pass
its check, so it is parked in the DLQ`. It is parked as its claim check (one
that could not be read, as it came), its outcome is `DLQ`, and its offset is
committed after the DLQ accepted it. The reasons:

- `does not match its reference's SHA-256`, `is larger than its reference`, or
  `Payload object size mismatch for <ref>`: the object was changed or replaced
  after it was stored.
- `Payload object does not exist: <ref>`: the object store answered that the
  key does not exist; the object was deleted, or its lifecycle expiry removed it.
- `Payload reference has expired: <ref>`: the reference's expiry has passed.
- `Invalid payload-reference document`, `Invalid S3 payload reference`, or
  `Payload-reference header <name> does not match its document`: the claim
  check is malformed.

Restore the object as it was stored and resend the parked record, or send the
message again from its source; an expired reference can only be sent again
from its source. If the DLQ refuses the record, the ERROR `Message <id> could
not be parked, so it is read again: <reason>` follows, and the record is read
again like any input that is not parked.

Any other reason may pass: the outcome is `FAILED`, the Kafka offset is not
committed, and the record is read again, with a growing wait of up to 30
seconds; it **blocks its partition** meanwhile, since later records of that
partition are not processed before it. This is also the case for every failure
on the first attempt of a streamed input, whose record is stored again when it
is read again. The reasons:

- `Payload object is unavailable: <ref>`: the object store cannot be reached,
  refuses access or times out, or the bucket does not exist. Check the
  endpoint, the workload identity and the bucket setting. On AWS S3 a missing
  key is answered with 403 when the workload may not list the bucket
  (`s3:ListBucket`), so a deleted object shows here too.
- `ended after N of its reference's M bytes`: the download stopped early, for
  example because a connection closed; the object itself may be intact.
- `Payload reference is outside the configured bucket` or `prefix`: the
  reference names another bucket or prefix than this deployment's settings.
- `could not be copied for checking to <directory>`: the object store failed
  while the object was read, or the spool directory is not writable or is full.
  Check the object store and the space in
  `basis.message-store.payload.spool-directory`.
- `cannot be checked now: checked copies of N bytes are in use`: other inputs'
  checked copies fill `basis.message-store.payload.spool-max-bytes`; the record
  is read again when they are done.

To get past a record that keeps failing this way, fix its cause, or move the
consumer group's offset for that partition past the record (with the Kafka
tools, while the integration is stopped) and resend another copy of the message
from its source.

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
  state: `1` held, `0` waiting, `2` uncoordinated, `3` stopped because the
  route needs its lease and there is no Key-Value Store. If no replica
  reports `1` or `2` for a route, nobody runs it.

## Replayed Route Has a Missing Response Body

The skipped write likely produces output required downstream. Classify it as
`OUTPUT_SIDE_EFFECT` so the adapter checkpoints and restores the response, or
explicitly redo it when the receiver contract makes repetition safe.

## Kafka Sink Produces Events but the Read Model Is Empty

The Kafka sink only publishes terminal events to `bip.message.status`. It does
not write Postgres. Deploy a separate lifecycle consumer that persists those
events, or use the direct Postgres sink.
