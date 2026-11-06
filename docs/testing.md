# Testing

## Adapter Repository

Run the complete unit and Testcontainers suite with JDK 21 and Docker:

```sh
./mvnw test
```

The suite exercises real Kafka, Postgres, and MinIO behavior where the boundary
matters. Generate public API documentation separately:

```sh
./mvnw -DskipTests javadoc:javadoc
```

### Restricted writer against the message recovery SQL

`RestrictedLoginWriterIT` runs the journal writer, the delivered-step store and
the replay service under separate runtime, operator and reader logins against
the optional message recovery SQL. It covers closure and history across late
callbacks and an explicit reopen, a removed payload that a writer cannot bring
back, retained history and delivered identity, replay admission refusals, an
uncertain replay that blocks payload removal, and a writer blocked behind an
operator's removal. It is not part of `./mvnw test`: it needs an explicit
selector and these inputs, from one reviewed provisioning revision, each with
its SHA-256:

- a directory with `002_adapter_v1_message_store.sql`,
  `004_contract_metadata.sql`, `005_tiered_payload_storage.sql`,
  `006_step_output_checkpoints.sql`, `011_message_recovery.sql` and
  `012_message_recovery_grants.psql`;
- a local `psql` executable, which runs the grants script;
- the cached PostgreSQL 17 image the test pins, and a fresh UUID as run id.

```sh
./mvnw test -Dtest=RestrictedLoginWriterIT \
  -Dbip.test.run-id="$(uuidgen)" -Dbip.test.recovery-sql-dir="$sql_dir" \
  -Dbip.test.sha256.002_adapter_v1_message_store.sql=… \
  -Dbip.test.sha256.004_contract_metadata.sql=… \
  -Dbip.test.sha256.005_tiered_payload_storage.sql=… \
  -Dbip.test.sha256.006_step_output_checkpoints.sql=… \
  -Dbip.test.sha256.011_message_recovery.sql=… \
  -Dbip.test.sha256.012_message_recovery_grants.psql=… \
  -Dbip.test.psql="$psql_path" -Dbip.test.psql-sha256=…
```

It tests the classes of the build and records them in
`target/restricted-login-writer/artifact-provenance.txt`. All six cases use
inline payloads; object-backed payloads are not covered. To qualify an exact
packaged adapter JAR instead, run the test from a separate test module that
depends on the installed artifact, so that only the JAR provides the adapter's
classes, and add `-Dbip.test.adapter-version` and `-Dbip.test.adapter-sha256`;
the test refuses classes that do not come from that JAR.

## Integration Testing Defaults

Prefer these repeatable approaches:

- a fresh Kafka consumer group for an isolated test run;
- an explicit consumer-group offset reset when that is the behavior under test;
- replay through the admin endpoint for production-like resend behavior;
- Testcontainers for local Kafka, Postgres, S3-compatible storage, and queue
  brokers.

## Guarded Test Profiles

`basis.message-store.test-profile` supports:

| Profile | Outcome persistence | Step persistence | Managed Kafka commit |
| --- | --- | --- | --- |
| `disabled` | configured behavior | configured behavior | enabled |
| `no-persistence` | log | no-op | enabled |
| `no-commit` | configured behavior | configured behavior | suppressed |
| `no-persistence-no-commit` | log | no-op | suppressed |

Every non-default profile refuses startup unless:

```properties
basis.message-store.allow-test-profiles=true
```

The adapter logs a prominent warning and exposes readiness data describing the
active profile and effective persistence/commit behavior. Production
configuration must keep test profiles disabled.

The profiles do not apply to the [Key-Value Store](key-value-store.md): its
calls still read and write `bip_key_value` under `no-persistence`.

No-commit mode does not make one running Kafka consumer continuously reread the
same record. Its in-memory position already advanced. Restart, seek, or reset
the group offset before expecting another delivery.

These profiles affect adapter persistence and adapter-managed Kafka commits.
They do not disable business database writes or external calls owned by the
integration.

## Local Versus Remote Evidence

Local tests can prove code behavior, broker offset handling, database writes,
claim checks, queue acknowledgement, partition assignment, and replacement
consumer redelivery. They cannot prove customer-cluster RBAC, Kubernetes
leader election, workload identity, network policy, deployed metrics, or
operator-tool wiring. Keep those scenarios in the batched remote acceptance
campaign rather than treating every feature as remotely verified immediately.
