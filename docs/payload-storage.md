# Payload Storage

Replay requires retained payload bytes or an immutable object reference. Audit
retention and payload retention are separate policies.

## Modes

| Mode | Small payload | Larger payload | Replay |
| --- | --- | --- | --- |
| `postgres-inline` | Postgres `bytea` | metadata only | only while inline |
| `tiered` | Postgres `bytea` | S3-compatible object | yes within limits/retention |
| `metadata-only` | metadata only | metadata only | no |

The library default is `postgres-inline` for compatibility. Platform production
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

The object limit is a deliberate platform ceiling, not an invitation to put
multi-gigabyte bytes into a Kafka record. Kafka receives a compact claim-check
document. The consuming adapter resolves the object into a streaming body.

## Object Metadata and Verification

For `s3_object`, Postgres stores the immutable object reference, size, SHA-256,
content type, and expiry. The adapter validates bucket, prefix, size, checksum,
and expiry before processing or replay. Missing, corrupt, expired, oversized,
or unavailable required objects fail processing; success is not recorded and a
consumed offset is not committed.

Object storage failure during required capture also fails the message. The
platform must not claim replayability without durable bytes.

## Retention

Replay does not extend the original payload expiry. Cleanup removes inline
bytes or the S3 object, sets `payload_deleted_at`, and marks the message
non-replayable while retaining lifecycle audit. Bucket lifecycle rules are a
safety net for orphaned objects, not the database source of truth.

## S3 Configuration

The adapter uses the Quarkus-managed S3 client. The platform supplies bucket,
region, credentials/workload identity, encryption, endpoint policy, and any
path-style setting. Local tests can use MinIO with an explicit endpoint.
Automatic LocalStack Dev Services are disabled by the adapter.

See [Configuration](configuration.md) for exact properties and environment
variables.
