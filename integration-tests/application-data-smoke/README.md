# Existing application database continuity

This synthetic fixture keeps the Message Store and an existing business cursor in
separate logical databases. It boots on the base Message Store contract without
a Key-Value Store table, reads and updates the existing cursor, and asserts that
unused optional KV starts no sweeper. The same fixture can run with adapter
0.3.5 or the 0.3.6 candidate using `-Dadapter.version=...`; do not change business
logic between the two runs. A passing candidate does not migrate application data.

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
