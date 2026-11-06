# Key-Value Store and metrics runtime fixture

This isolated Quarkus fixture checks the optional Key-Value Store against a
synthetic PostgreSQL 17 database. It also checks actual CDI/Micrometer
Prometheus HTTP export of the activity counters: admission counting by default,
and both counters when they are selected explicitly. The metrics
case supplies synthetic admission coordinates and broker-ACK metadata; it does
not start a Kafka producer, contact a broker or prove actual broker delivery.

The PostgreSQL test resource pins its image, loopback binding, resource limits,
run ownership and cleanup. The HTTP fixture uses a loopback ephemeral test port.
The explicit selection runs under its own test profile; the default case uses
no activity configuration at all.

Install the exact local candidate into the isolated qualification Maven repository
first, then run this module with the same repository and pinned Java 21 toolchain:

```sh
run_id="$(uuidgen)"
../../mvnw --batch-mode --no-transfer-progress test \
  -Dbip.test.run-id="$run_id" -DargLine="-Dbip.test.run-id=$run_id"
```

Use the qualified artifact matching `adapter.version`, never a floating remote
snapshot as proof of the current source. This fixture is prepared source until
its actual execution and cleanup receipt is recorded. Full qualification and
collection by installed operator tools remain independent gates.
