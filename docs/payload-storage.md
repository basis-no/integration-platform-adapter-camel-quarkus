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
the bucket, prefix, expiry and the object's size.

An input in object storage is checked in full before its route reads any of it,
on a replay of a claim-checked input and on the first attempt after the adapter
stored a streamed input. The adapter copies the object to a local file in
a subdirectory named after the host in
`basis.message-store.payload.spool-directory` (the JVM's temporary directory by
default; owner-only permissions), so that replicas that share a volume never
touch each other's copies, while it checks the object's size and SHA-256 against the reference,
and only then gives the route a stream of that checked copy. Memory stays
bounded by a 64 KiB copy buffer, whatever the object's size. The copy is
deleted when the exchange is done, and at once when the check fails; on Linux
its directory entry is removed as soon as the copy is created, so no other
process can open it, and the space is freed when the exchange is done. A copy left by a process that stopped
without cleaning up, such as after a crash, is deleted when the application
starts again.

The checked copies one process holds at once are bounded by
`basis.message-store.payload.spool-max-bytes`, by default the same as
`object-max-bytes`, so that an object of the largest accepted size always fits
when it is the only one. In `tiered` mode startup refuses a smaller value. Give
the spool directory room for that much: a pod's ephemeral storage, or a volume
mounted at the directory.

An output checkpoint is checked completely before it is restored; see
[Output-Producing Side Effects](steps-and-idempotency.md#output-producing-side-effects).

A missing, expired, changed, truncated or oversized object, an object store
failure while it is read, a spool directory that is full or not writable, and
checked copies that already fill `spool-max-bytes`, all fail the input before
its route runs: no processor sees the payload, nothing is parked, its outcome is
recorded `FAILED` and a consumed Kafka offset is not committed, so the record is
read again. The `FAILED` outcome keeps the message's claim check.

A changed, truncated, missing or expired object does not heal by itself, so its
record is read again, with a growing wait, until the object is restored, and
it blocks its partition meanwhile. See
[Troubleshooting](troubleshooting.md#s3-replay-fails) for how to get past it.

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
