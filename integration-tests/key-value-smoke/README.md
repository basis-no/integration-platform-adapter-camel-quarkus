# Key-Value Store and metrics runtime fixture

This isolated Quarkus fixture checks the optional Key-Value Store against a
synthetic PostgreSQL 17 database. It also checks actual CDI/Micrometer
Prometheus HTTP export for explicitly selected activity counters. The metrics
case supplies synthetic admission coordinates and broker-ACK metadata; it does
not start a Kafka producer, contact a broker or prove actual broker delivery.

The PostgreSQL test resource pins its image, loopback binding, resource limits,
run ownership and cleanup. The HTTP fixture uses a loopback ephemeral test port.
Only this fixture enables activity export; library defaults remain disabled.
Existing application database compatibility without KV or metrics is checked by
`../application-data-smoke` separately.

Install the exact local candidate into the isolated qualification Maven repository
first, then run this module with the same repository and pinned Java 21 toolchain:

```sh
../../mvnw --batch-mode --no-transfer-progress test
```

Use the qualified artifact matching `adapter.version`, never a floating remote
snapshot as proof of the current source. This fixture is prepared source until
its actual execution and cleanup receipt is recorded. Full certification and
installed agent/platform collection remain independent gates.
