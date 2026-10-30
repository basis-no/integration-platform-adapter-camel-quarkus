# Maintenance snapshot validation application

The default profile is a local startup test with a stopped Kafka input and no
database. The explicitly selected `acceptance` profile consumes synthetic input
from operator-provisioned Kafka and records outcomes in PostgreSQL. It has no
connection to a business receiver. This is a test application, not a template or
a production integration. Its internal receiver accepts `PASS ` and rejects all
other payloads. Errors propagate to the Kafka input's platform recovery handler.

## Build the published candidate

Use JDK 21, the exact reviewed source commit, and the timestamped snapshot version
recorded after publication. `ADAPTER_VERSION` may be that timestamped version,
for example the value in Sonatype's snapshot metadata; do not invent it. Using
only `0.3.4-SNAPSHOT` can resolve newer bytes on a later rebuild.

From the adapter repository root:

```sh
: "${ADAPTER_VERSION:?Set the verified published candidate version}"
./mvnw -U -f integration-tests/maintenance-smoke/pom.xml \
  -Dadapter.version="$ADAPTER_VERSION" clean verify
: "${JAVA_RUNTIME_IMAGE:?Set the approved Java 21 runtime image with @sha256 digest}"
: "${VALIDATION_IMAGE:?Set the approved validation image tag}"
docker build --build-arg JAVA_RUNTIME_IMAGE="$JAVA_RUNTIME_IMAGE" \
  -t "$VALIDATION_IMAGE" integration-tests/maintenance-smoke
```

Record the source SHA, resolved adapter version/JAR hash and image digest.
Publishing an image and deploying it are separate authorized operations.

## Runtime bindings

An authorized operator provisions a dedicated, disposable input topic (one
partition), DLQ topic, consumer group and database/schema before deployment.
Use the normal platform path and its approved Kafka authentication bindings.
Do not reuse business topics/groups or the ongoing development validation app.
No public ingress is needed. Restrict the inherited admin resend endpoint to the
approved operator path; the test image adds no authentication layer.

Set `QUARKUS_PROFILE=acceptance` and provide:

- `ACCEPTANCE_NAME`: unique test integration name.
- `ACCEPTANCE_INPUT_TOPIC`, `ACCEPTANCE_DLQ_TOPIC`, `ACCEPTANCE_GROUP`: dedicated resources.
- `KAFKA_BOOTSTRAP_SERVERS` and the adapter's required Kafka security bindings.
- `POSTGRES_JDBC_URL`, `POSTGRES_USERNAME`, `POSTGRES_PASSWORD`: disposable Message Store database.

Verify effective manual commit, automatic commit disabled and stop-on-first-error.
Never disable security to make the test work. No environment credentials belong
in this source or the evidence returned from the operator.

## Small runtime acceptance

With `kcat` configured by the operator's credential file, send three synthetic
inputs in order. Use a fresh group and empty input topic so expected offset is 3.

```sh
: "${KAFKA_CLIENT_CONFIG:?Set the protected kcat configuration file}"
: "${ACCEPTANCE_INPUT_TOPIC:?Set the dedicated input topic}"
printf 'PASS first\nFAIL rejected\nPASS following\n' | \
  kcat -F "$KAFKA_CLIENT_CONFIG" -P -t "$ACCEPTANCE_INPUT_TOPIC"
```

Observe three source lifecycle rows: COMPLETED, DLQ, COMPLETED. Confirm only
`FAIL rejected` is parked and its source attempt retains the original bytes.
There should be no independent lifecycle row for `receiver`. The dedicated
consumer group's committed position must reach 3 and survive a normal restart
without reprocessing those records. Record exact images, counts and identifiers.

For replay-after-repair, set `ACCEPTANCE_ACCEPT_REJECTED=true` on this disposable
app through the approved configuration path and roll it out with the same group
and topics. This enables the synthetic receiver's repair mode. Through the
approved operator/admin path, resend the failed **source** message once. Confirm
its current status becomes COMPLETED on attempt 2, attempt 1 remains DLQ history,
and the committed position reaches 4. The original DLQ Kafka record can remain;
its presence does not mean another resend occurred. Do not redo delivered steps
or change the consumer group to perform this check.

The adapter's local Kafka/PostgreSQL tests cover failed publication, failed
journaling, historical unsafe replay and later-record commit ordering. Those
are not automatically target-cluster results. Any runtime outage test must use
only these disposable resources and a separately approved operator procedure;
never interrupt shared Kafka/PostgreSQL or change business consumer offsets.
