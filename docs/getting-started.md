# Getting Started

## Prerequisites

- JDK 21
- a Camel Quarkus integration generated from a supported platform template
- Kafka and Postgres enabled in the integration catalog for the normal v1 path
- the platform-provisioned message-store schema

The template should pin the adapter dependency. For a standalone experiment,
add the Maven Central artifact shown in the repository [README](../README.md).

## Write a Business Route

A normal integration consumes its canonical input topic and contains business
logic only:

```yaml
- route:
    id: receive-order
    from:
      uri: "kafka:bip.orders.in?groupId=orders"
      steps:
        - to:
            id: create-order
            uri: "https://orders.example.test/api/orders"
```

The adapter automatically selects Kafka consumer routes as message boundaries.
It installs retry/DLQ behavior, records the terminal attempt, and performs the
manual Kafka commit. Do not add a commit processor or message-store SQL to the
route.

Use a stable Camel node id for an external operation. If automatic
classification cannot safely determine its replay behavior, classify the exact
id in configuration:

```properties
basis.message-store.step.side-effect-ids=create-order
```

Read [Steps and idempotency](steps-and-idempotency.md) before classifying an
operation: authentication, reads, writes, and output-producing writes have
different replay behavior.

## Production Configuration

The platform chart supplies connection details and the production mode. The
normal direct-Postgres profile is conceptually:

```properties
basis.message-store.sink=postgres
basis.message-store.step-outcome-store=postgres
basis.message-store.payload.mode=tiered
basis.message-store.replay.enabled=true
basis.message-store.failure-mode=dlq
```

Integration developers should not duplicate platform-owned values such as the
deployment replica count, Postgres credentials, payload bucket, or canonical
DLQ topic in repository configuration. See the
[configuration reference](configuration.md) for ownership and environment
variable mappings.

## Verify Locally

```sh
./mvnw test
```

For application-level testing, use a fresh Kafka consumer group or a deliberate
dashboard replay. Guarded test profiles exist when the test specifically needs
to suppress persistence or broker commits; see [Testing](testing.md).

## Next Reading

- [Message lifecycle](message-lifecycle.md)
- [Retry, DLQ, and replay](retry-dlq-replay.md)
- [Ingress and scaling](ingress-and-scaling.md)
