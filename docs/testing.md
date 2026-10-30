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

## Integration Testing Defaults

Prefer these repeatable approaches:

- a fresh Kafka consumer group for an isolated test run;
- an explicit consumer-group offset reset when that is the behavior under test;
- dashboard replay for production-like resend behavior;
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
active profile and effective persistence/commit behavior. Platform production
configuration must reject test profiles.

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
dashboard wiring. Keep those scenarios in the batched remote acceptance
campaign rather than treating every feature as remotely certified immediately.
