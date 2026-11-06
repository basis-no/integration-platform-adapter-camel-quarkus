# Payload Storage

Replay requires retained payload bytes or an immutable object reference. Audit
retention and payload retention are separate policies.

## Modes

| Mode | Small payload | Larger payload | Replay |
| --- | --- | --- | --- |
| `postgres-inline` | Postgres `bytea` | metadata only | only while inline |
| `tiered` | Postgres `bytea` | S3-compatible object | yes within limits/retention |
| `metadata-only` | metadata only | metadata only | no |

The library default is `postgres-inline` for compatibility. Production
configuration should select `tiered`; adding the dependency should not
silently require local S3 credentials.

## Limits

- default inline threshold: 1 MiB;
- enforced inline ceiling: 16 MiB;
- default object payload limit: 10 GiB;
- enforced object ceiling: 50 GiB;
- default multipart part size: 8 MiB;
- default payload retention: 30 days.

The inline threshold protects Postgres WAL, replication, backup, and memory
behavior. Do not raise it to accommodate large business payloads. In tiered
mode, exceeding the inline threshold selects object storage; it does not make
the message metadata-only.

The object limit is a deliberate ceiling, not an invitation to put
multi-gigabyte bytes into a Kafka record. Kafka receives a compact claim-check
document. The consuming adapter resolves the object into a streaming body.

## Parked Payload

A DLQ record carries the consumed input, also when the route changed it. The
adapter keeps one copy of the input at route start: the claim check of an
object payload, the inline replay copy, or, when replay storage does not keep
the payload (`metadata-only`, replay disabled, or above the inline threshold
outside `tiered` mode), a copy for parking, only for a Kafka input. A byte array
input is copied, so a route that changes it in place changes neither the parked
record nor the stored payload or its SHA-256. A String input is not copied. A
body of another type, such as a stream outside `tiered` mode, is parked as the
route left it. A byte array input therefore holds one extra copy of its size
until its exchange completes, unless it is stored as an object.

## Object Metadata and Verification

For `s3_object`, Postgres stores the immutable object reference, size, SHA-256,
content type, and expiry. Before processing or replay, the adapter validates
the bucket, prefix, expiry and the object's size. An input's SHA-256 is checked
when the route reads the payload stream to its end, and a mismatch fails that
read: a route that acts on only part of an object payload can use bytes that
were not checked yet
([#60](https://github.com/basis-no/integration-platform-adapter-camel-quarkus/issues/60)).
An output checkpoint is checked completely before it is restored; see
[Output-Producing Side Effects](steps-and-idempotency.md#output-producing-side-effects).
A missing, expired, oversized or unavailable required object, and a corrupt
one that is detected, fails processing; success is not recorded and a consumed
offset is not committed. Until #60 is fixed, a corrupt input that the route
does not read to its end can still complete and be committed.

Object storage failure during required capture also fails the message. The
adapter never claims replayability without durable bytes.

## Retention

With the `postgres` sink, a replay captures an inline payload again and resets
its expiry, capped at first receipt plus twice the retention. First receipt is
the earliest outcome this integration recorded for the message. An S3 object
payload keeps its first expiry together with its object.

Cleanup removes inline bytes or the S3 object, sets `payload_deleted_at`, and
marks the message non-replayable while retaining lifecycle audit. Bucket
lifecycle rules are a safety net for orphaned objects, not the database source
of truth.

## S3 Configuration

The adapter uses the Quarkus-managed S3 client. The deployment supplies bucket,
region, credentials/workload identity, encryption, endpoint policy, and any
path-style setting. Local tests can use MinIO with an explicit endpoint.
Automatic LocalStack Dev Services are disabled by the adapter.

See [Configuration](configuration.md) for exact properties and environment
variables.
