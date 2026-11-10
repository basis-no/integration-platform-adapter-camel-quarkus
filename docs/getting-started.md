# Getting Started

## Prerequisites

- JDK 21
- a Camel Quarkus integration
- Kafka and Postgres for the normal production path
- the message-store schema, provisioned by the database operator

Add the Maven Central artifact shown in the repository [README](../README.md)
and pin its version in the integration's build.

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

The deployment supplies connection details and the production mode. The
normal direct-Postgres profile is conceptually:

```properties
basis.message-store.sink=postgres
basis.message-store.step-outcome-store=postgres
basis.message-store.payload.mode=tiered
basis.message-store.replay.enabled=true
basis.message-store.failure-mode=dlq
```

Integration developers should not duplicate deployment-owned values such as the
deployment replica count, Postgres credentials, payload bucket, or canonical
DLQ topic in repository configuration. See the
[configuration reference](configuration.md) for ownership and environment
variable mappings.

### Health and Readiness

The adapter adds no health check for its Kafka consumers. With SmallRye Health
alone, readiness stays Ready after a consumer stops polling. Add Camel's health
bridge, under the application's own Camel Quarkus BOM:

```xml
<dependency>
  <groupId>org.apache.camel.quarkus</groupId>
  <artifactId>camel-quarkus-microprofile-health</artifactId>
</dependency>
```

With the bridge, readiness turns NotReady (HTTP 503) while a consumer is stopped
or cannot reach the broker, and Ready again after it reconnects; an idle
consumer is Ready. While the consumer reconnects, readiness can alternate before
it settles. NotReady does not restart a pod, so alert on a sustained NotReady.

Set the readiness probe's timeout to 3–5 seconds: while the consumer
reconnects, Camel's Kafka health check can take more than 2 seconds to answer.

The adapter's own readiness check reports only an active test profile; see
[Testing](testing.md).

## Verify Locally

```sh
./mvnw test
```

For application-level testing, use a fresh Kafka consumer group or a deliberate
replay through the admin endpoint. Guarded test profiles exist when the test specifically needs
to suppress persistence or broker commits; see [Testing](testing.md).

## Next Reading

- [Message lifecycle](message-lifecycle.md)
- [Retry, DLQ, and replay](retry-dlq-replay.md)
- [Ingress and scaling](ingress-and-scaling.md)
