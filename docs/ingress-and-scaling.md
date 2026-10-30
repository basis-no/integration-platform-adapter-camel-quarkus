# Ingress and Scaling

## Supported Shapes

| Source shape | Recommended boundary | Replay | Replica rule |
| --- | --- | --- | --- |
| Kafka topic | Kafka consumer route | Full v1 | same consumer group |
| File/FTP/SFTP | normalize to canonical Kafka topic | from Kafka boundary | one replica or `master:` |
| Timer/database poll | normalize each real source item to Kafka | from Kafka boundary | one replica or `master:` |
| JMS/SJMS queue | normalize to Kafka | from Kafka boundary | broker competing consumers |
| Synchronous HTTP | explicit audit-only preview | disabled | application-specific |

## Kafka Consumers

All replicas of one integration should use the same Kafka consumer group.
Kafka assigns a partition to at most one member of that group at a time. More
replicas can process separate partitions concurrently; records with the same
key remain in one partition and retain partition order.

A different consumer group is an independent subscriber and receives its own
delivery of every relevant record. The current v1 model is a single-delivery
profile and must not be used to represent several independent receivers under
one message id without the planned delivery-identity contract.

## Non-Kafka Normalization

Asynchronous non-Kafka sources should hand off to the canonical input topic:

```text
source item -> normalization route -> bip.<integration>.in
                                  -> Kafka business route/message store
```

Configure exact normalization route ids and a durable step store:

```properties
basis.message-store.ingress.normalization-routes=file-to-kafka
basis.message-store.step-outcome-store=postgres
```

```yaml
- route:
    id: file-to-kafka
    routeConfigurationId: bip-ingress-normalization
    from:
      uri: "file:/in?move=.done/${file:name}"
      steps:
        - to:
            id: publish-canonical-message
            uri: "kafka:bip.orders.in"
```

The adapter derives a stable message id from integration, route, and source
identity. JMS message ids and stable file metadata are supported. Polling and
custom queue components must set `x-bip-source-event-id` from a durable source
identifier; a generated timestamp is not stable.

The source acknowledges only after Kafka publish and durable publish-ledger
recording succeed. This is not an atomic transaction across source, Kafka, and
Postgres: a crash after Kafka accepts the record but before ledger completion
can duplicate the Kafka record. Both copies retain the same message id so the
downstream idempotency model can contain the duplicate.

Empty polls produce no source event and therefore no message-store row. Manual
replay starts at Kafka and does not reread the original file, queue, or poll.

## Singleton Polling

The platform chart injects `basis.message-store.deployment-replicas` from the
Kubernetes Deployment. An uncoordinated polling route is accepted only with one
declared replica. With multiple replicas, wrap the source in Camel `master:`
and configure `camel-quarkus-kubernetes-cluster-service` plus its Kubernetes
leader-election permissions:

```yaml
from:
  uri: "master:file-ingress:file:/in?move=.done/${file:name}"
```

`jms:queue:` and `sjms2:queue:` accept multiple replicas because the broker
coordinates competing queue consumers. Missing replica metadata or an unsafe
multi-replica poll fails startup rather than risking duplicate discovery.

## HTTP Audit-Only Preview

An explicitly selected `platform-http:` route may record terminal audit without
claiming Kafka commit, DLQ, or replay semantics:

```properties
basis.message-store.boundary-routes=receive-order
basis.message-store.failure-mode=record-failed
basis.message-store.replay.enabled=false
basis.message-store.sink=postgres
```

Success records `COMPLETED`; technical or HTTP 4xx/5xx failure records `FAILED`
and propagates to the caller. Payload is metadata-only and the dashboard must
not offer resend. This is an implemented Camel preview, not certified
cross-runtime contract v1, and cannot be mixed with adapter-managed Kafka
boundaries in the current global-policy implementation.
