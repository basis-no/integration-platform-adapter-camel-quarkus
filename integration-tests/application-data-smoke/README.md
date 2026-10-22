# Existing application database continuity

This synthetic fixture keeps the Message Store and an existing business cursor in
separate logical databases. It boots on the base Message Store contract without
a Key-Value Store table, reads and updates the existing cursor, and asserts that
unused optional KV starts no sweeper. The same fixture can run with adapter
0.3.5 or the 0.3.6 candidate using `-Dadapter.version=...`; do not change business
logic between the two runs. A passing candidate does not migrate application data.
The fixture supplies a synthetic loopback Kafka bootstrap value for the adapter's
required CDI configuration; it starts no Kafka route or broker. Its HTTP listener
also binds to loopback on an ephemeral port.

Use a freshly cached pinned PostgreSQL 17 image and a unique UUID run ID. One
loopback server is capped at256MiB memory/swap,1CPU/64PIDs and stops on partial
startup. With Docker available and the selected adapter installed locally:

```sh
run_id="$(uuidgen)"
./mvnw -f integration-tests/application-data-smoke/pom.xml \
  -Dadapter.version=0.3.6-SNAPSHOT -Dbip.test.run-id="$run_id" verify
```

Runtime and baseline results remain separate until executed against the exact
artifacts. The fixture uses no customer data, remote JDBC URL or live platform.

## Restricted LOGIN recovery writer matrix

`RestrictedLoginWriterIT` reuses this module's JDBC and Testcontainers dependencies.
It is plain JUnit and requires an explicit Surefire selector; the ordinary
continuity command above does not run it. Run the unchanged fixture separately
with the packaged 0.3.5 adapter and the packaged 0.3.6-SNAPSHOT candidate. The
existing continuity fixture must also pass separately for each version; recovery
results do not establish application-database or optional KV compatibility.

Prepare these inputs before the supervised run:

- A local, exact adapter JAR and its SHA-256, with the chosen version installed
  in the selected Maven repository. The fixture checks the actual loaded JAR,
  embedded version and shared writer/replay/step code origin; `target/classes`
  does not qualify. Record the source commit, build recipe and dependency closure
  separately, especially for a mutable snapshot coordinate.
- The platform-owned `002_adapter_v1_message_store.sql`,
  `011_message_recovery.sql` and `012_message_recovery_grants.psql` from one
  reviewed revision in a supplied directory, with independently recorded SHA-256
  values. The fixture never downloads or replaces provisioning SQL.
- A local executable `psql` and its SHA-256, plus the already cached PostgreSQL
  17 image pinned in the fixture. No image download is allowed by the fixture.
- A fresh UUID, a local engine and an outer execution budget/cleanup owner.
  Schema/grant creation happens only inside the fixture's disposable database.

For each version, use the existing runner with all inputs explicitly supplied
(the named variables below refer to those prepared inputs):

```sh
./mvnw -f integration-tests/application-data-smoke/pom.xml \
  -Dtest=RestrictedLoginWriterIT -Dadapter.version="$adapter_version" \
  -Dbip.test.adapter-version="$adapter_version" \
  -Dbip.test.adapter-sha256="$adapter_sha256" \
  -Dbip.test.run-id="$run_id" -Dbip.test.recovery-sql-dir="$sql_dir" \
  -Dbip.test.sha256.002_adapter_v1_message_store.sql="$base_sql_sha256" \
  -Dbip.test.sha256.011_message_recovery.sql="$recovery_sql_sha256" \
  -Dbip.test.sha256.012_message_recovery_grants.psql="$grants_sql_sha256" \
  -Dbip.test.psql="$psql_path" -Dbip.test.psql-sha256="$psql_sha256" test
```

The six cases call the actual journal writer and delivered-step store under a
runtime LOGIN. Separate operator and reader LOGIN sessions install/use the exact
grant boundary and assert denied authority. They cover older callbacks after
closure and explicit reopen, same/new callbacks after payload removal, retained
history/delivered identity/expiry, replay admission refusal, uncertain-audit
removal refusal, and an actual writer blocked behind operator removal. The last
case waits for a database-observed lock barrier before committing removal. The
uncertain-audit case inserts SQL fixture state; it does not publish through Kafka.

The replay case uses the existing package-private constructor from a test in the
same package, with an unstarted Kafka input route and empty producer bootstrap.
It requires the exact database admission error for a closed message and the
client's payload refusal after removal. There is no injectable producer factory:
this is not an instrumented zero-producer-call proof, accepted replay or broker
acknowledgement. Those need separate broker-backed qualification. The fixture
does not change the SDK API, product SQL/grants or deployment RLS policies.

Each run owns one capped loopback PostgreSQL server, separate synthetic LOGINs,
bounded JDBC calls and one bounded callback worker. Setup/test/teardown timeouts
are 120/120/30 seconds, with a separate 20-second psql wait and mandatory reap.
JUnit timeouts do not replace outer supervision or positive container/process
cleanup. Preserve failed temporary logs and Surefire results; record each
version's executed case count, failures/skips, JAR/SQL/psql identities and cleanup
receipt. This source preparation has not been compiled or executed and supplies
no runtime qualification result. Object payloads, retained output checkpoints,
accepted replay callbacks, full RLS/migration/rollback and other runtimes remain
separate requirements.
