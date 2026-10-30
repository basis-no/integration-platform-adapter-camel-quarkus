# Troubleshooting

## No Message Row Appears

- Confirm the route is a selected business-message boundary. `auto` selects
  Kafka consumer routes only.
- Check whether `basis.messageStore.recordMessage=false` was set.
- Confirm the effective sink is `postgres`, not `log` or the Kafka status sink.
- Check for a test profile that disables persistence.
- Verify the platform schema and datasource binding.

## Message Completes but Kafka Offset Does Not Advance

- Check sink, DLQ, and step-store errors before looking at the business route.
- Confirm `allow-manual-commit=true` and `auto-commit-enable=false`.
- Check whether the guarded `no-commit` test profile is active.
- A running no-commit consumer advances its in-memory position; inspect broker
  group offsets rather than expecting an immediate duplicate delivery.

## Route Fails at Startup

Common fail-fast causes include:

- unsupported sink, payload, failure-mode, or test-profile value;
- non-default test profile without explicit opt-in;
- Postgres modes without a resolvable datasource;
- tiered mode without a payload bucket/S3 client;
- conflicting step classification ids;
- normalization route missing `bip-ingress-normalization`;
- normalization route also selected as a lifecycle boundary;
- normalization without the durable Postgres step store;
- unsafe polling with missing or multiple declared replicas;
- contract major mismatch in `bip_contract`.

Treat these as configuration errors. Do not bypass them in business routes.

## Ambiguous Step Warning

The adapter cannot prove whether the endpoint is a safe read or a durable write.
Give the send a stable node id and classify that exact id. Do not suppress the
warning by guessing from the id text.

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
the deployment count in application properties; the chart injects it.

## Replayed Route Has a Missing Response Body

The skipped write likely produces output required downstream. Classify it as
`OUTPUT_SIDE_EFFECT` so the adapter checkpoints and restores the response, or
explicitly redo it when the receiver contract makes repetition safe.

## Kafka Sink Produces Events but Dashboard Is Empty

The Kafka sink only publishes terminal events to `bip.message.status`. It does
not write Postgres. Deploy the separate lifecycle consumer required by the
future central model, or use the direct Postgres sink for v1.
