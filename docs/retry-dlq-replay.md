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
platform attempt; it adds diagnostics to that attempt.

## Terminal Kafka Failure

`basis.message-store.failure-mode=dlq` is required for Kafka lifecycle
boundaries. After retries are exhausted, the adapter sends the original
captured payload to `basis.message-store.dlq-uri`, preserves the envelope, adds
`x-bip-error` and `x-bip-dlq-at`, records `DLQ`, and only then commits the input
offset.

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

The dashboard supplies operator identity and reason through headers or the JSON
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

## Contract Startup Gate

With the Postgres sink, startup reads the highest contract major from
`bip_contract`. A different major fails startup. A missing table or empty row
uses the contract-v1 rollout grace with a prominent warning; other database
errors are not treated as a missing schema.
