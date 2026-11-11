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
stored a streamed input. The adapter copies the object to a local file in a
subdirectory named after the host in
`basis.message-store.payload.spool-directory` (the JVM's temporary directory by
default), so that replicas that share a volume never touch each other's copies,
while it checks the object's size and SHA-256 against the reference, and only
then gives the route a stream of that checked copy. The check holds one 64 KiB
buffer in memory, whatever the object's size. That bound is the check's, not the
route's: Camel's stream caching, on by default, reads the whole stream into its
cache at the route's first node, in memory unless its spooling to disk is turned
on (`camel.main.stream-caching-spool-enabled`). For large objects, turn that on
or read the stream without stream caching. The copy is closed and deleted when
the boundary exchange is done, so with stream caching off a route must read the
stream itself before it hands the exchange to work that reads it later, such as
a `wireTap` or a `seda:` queue. The copy is also deleted at once when the check
fails; on Linux its directory entry is removed as soon as the copy is created,
so no other process can open it, and the space is freed when the exchange is
done. A copy left by a process that stopped without cleaning up, such as after a
crash, is deleted when the application starts again.

Where the file system supports POSIX permissions, the adapter creates the
subdirectory owner-only (`rwx------`) and each copy owner-only (`rw-------`);
a directory that already exists, such as a mounted volume, keeps its own
permissions.

The checked copies one process holds at once are bounded by
`basis.message-store.payload.spool-max-bytes`, 10 GiB by default, the same as
the default `object-max-bytes`, so that an object of the largest accepted size
always fits when it is the only one. In `tiered` mode startup refuses a smaller
value, so raise both together. Mount a dedicated, size-limited volume at the
spool directory, with room for `spool-max-bytes`: on Linux a copy has no
directory entry while it is read, and such a file may not count toward a pod's
ephemeral-storage accounting.

An output checkpoint is checked completely before it is restored; see
[Output-Producing Side Effects](steps-and-idempotency.md#output-producing-side-effects).

An input whose payload cannot be captured or checked never reaches its route: no
processor sees the payload. What happens next depends on whether the failure can
pass.

A Kafka input that arrives as a claim check, on a replay or a DLQ resend, and
that can never pass its check is parked in the DLQ as its claim check, and its
offset is committed only after the DLQ accepted it, as for any parked record:
the object's SHA-256 does not match the reference, the object store reports
another size, or the object has more bytes than the reference; the object store
answers that the object's key does not exist; the reference has expired; or the
claim check is malformed or its headers do not match its document. Such a claim
check is parked as it came, when Kafka delivered it as bytes or text, and its
row is metadata-only and not replayable. Its outcome is `DLQ`, with the claim
check kept. Once the object is restored, resend the parked record; an expired
one must be sent again from its source.

Every other failure may pass, so the input fails, its outcome is recorded
`FAILED` and a consumed Kafka offset is not committed, so the record is read
again, with a growing wait, and blocks its partition meanwhile: an object store
that cannot be reached, refuses access or times out, a bucket that does not
exist, a download that ends before the reference's size (a connection that
closes early ends the stream too), a reference outside the configured bucket or
prefix, a spool directory
that is full or not writable, checked copies that already fill
`spool-max-bytes`, and any failure on the first attempt of a streamed input,
whose Kafka record is still the source and is stored again when it is read
again. The `FAILED` outcome keeps the message's claim check. On an HTTP audit-only
boundary every failure is handed back to the caller and the row stays
metadata-only and not replayable, as it does when the check passes.

The adapter decides by what the store and the check answer, so a cause outside
the object can still look final, and the records it reads are then parked: a
host clock far ahead makes references look expired; an endpoint or account
setting that points at another store with a bucket of the same name, or a
replica that does not have the object yet, answers that the key does not exist.
Resend such records from the DLQ while their objects exist. The other way
round, AWS S3 answers 403, not 404, for a missing key when the workload may not
list the bucket (`s3:ListBucket`); a deleted object then looks like a refused
access and is read again, so grant list permission on the payload prefix for
it to be parked. See [Troubleshooting](troubleshooting.md#s3-replay-fails).

Object storage failure during required capture also fails the message. The
adapter never claims replayability without durable bytes.

## Retention

With the `postgres` sink, a replay captures an inline payload again and resets
its expiry, capped at first receipt plus twice the retention. First receipt is
the earliest outcome this integration recorded for the message. An S3 object
payload keeps its first expiry together with its object: the summary row holds
it, and an object attempt row written by this version takes it from the summary
in the same transaction, so history and summary show the same expiry for its
object attempts. A message whose earlier attempts were inline, or written by an
older adapter version, keeps those attempt rows' own expiries.

The cap counts from the earliest outcome still recorded. Recording the same
attempt again replaces that attempt's outcome time, for example when the
original record is consumed again after a replay. If it was the earliest
outcome, a later replay's cap starts from the next earliest one, so it can
reach past first receipt plus twice the retention by up to the time between
those two outcomes.

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
