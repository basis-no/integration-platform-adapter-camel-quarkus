# Ingress and Scaling

## Supported Shapes

| Source shape | Recommended boundary | Replay | Replica rule |
| --- | --- | --- | --- |
| Kafka topic | Kafka consumer route | full | same consumer group |
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
delivery of every relevant record. The current model is a single-delivery
profile and must not be used to represent several independent receivers under
one message id; that needs a separate delivery identity, which the adapter does
not provide.

An HTTP intake that publishes to Kafka must remove the caller's headers first,
for example with `removeHeaders("*")` and then setting only the headers the
message needs. Camel copies every request header onto the exchange, and the
Kafka producer publishes them with the message, including `Authorization`, API
keys and cookies, unmasked. Anyone who can read the topic can read them. A
Kafka send that published a header with a credential-like name
(`Authorization`, cookies, authentication challenges, and names containing
secret, token, password, credential, session, signature or auth, or ending in
`-key` or `_key`; not `x-bip-*` or `Idempotency-Key`) logs a warning once per
route and header name. The warning names the header, never its value, and says
how to remove it. What is published does not change. The check runs after a
successful `to` or `toD`, against the endpoint the send used, including a `toD`
that chooses its component only when it sends. It follows that endpoint's
header filter strategy and counts only text, byte array, `Long`, `Integer`,
`Double` and `Boolean` values, which Camel's header serializer writes as they
are. An endpoint whose header filter strategy or header serializer is an
application class, or whose filter holds an application collection, is not
checked: the adapter does not call application code to find out what was
published. Nor is a send through a `toD` with `cacheSize=-1` to an endpoint
that is not yet registered: Camel creates such an endpoint for the one send
without registering it, so the adapter cannot find it afterwards. An endpoint
that is already registered is reused and checked. At most 100 header names are
reported per route; one more line then says that further names are not logged.

No send to an HTTP receiver forwards a consumed record's credential-like
headers, by the same name rule, while they still hold the record's bytes. A
credential header the route sets itself is text and is sent as before; set it
as text, for example with
`setHeader("Authorization", header("Authorization").convertToString())`, to
forward a caller's credential deliberately. Nor does any such send forward
Camel's Kafka consumer headers named `kafka.*`. On camel-kafka versions before
4.14.8, 4.18.3 and 4.21.0, these include `kafka.HEADERS`, which lists the bytes
of every record header, credentials included, and the record key, topic,
partition, offset and timestamp. Later Camel versions name them `CamelKafka*`,
and Camel's HTTP producers do not send those names. A receiver that needs the
record key or topic should get it from a header the route sets itself.

The adapter adds these headers to the header filter of every `http`, `https`,
`netty-http` and `vertx-http` endpoint the context registers, together with the
endpoint's own filter, such as one set with `headerFilterStrategy=#name`. The
producer then leaves them out of the request, whichever EIP sends: `to`, `toD`,
`enrich`, `wireTap`, `recipientList`, `routingSlip`, `multicast` and a
`ProducerTemplate`. The exchange keeps the headers. A `toD` URI, a
`rest:` placeholder such as `{kafka.TOPIC}`, a later routing slip step, the
route and the routes it calls through internal endpoints such as `direct:`
still read them, and the record the adapter parks in the DLQ keeps its key.
Each credential header left out logs a warning once per step or route and
header name, never its value. A tracked `to` or `toD` through Camel's `http`
component, and a `toD` that chooses its component only when it sends, such as
`${header.receiverUrl}`, also withhold the credential headers around the send
itself.

An HTTP endpoint whose header filter the adapter cannot extend fails when it is
created, so its route does not start. An endpoint without a header filter sends
no headers and is left as it is. Not covered: an endpoint that Camel creates for
a single send without registering it, such as one from a `toD` with
`cacheSize=-1`; an endpoint whose header filter is replaced after the endpoint
was registered; a `rest:` send, whose producer replaces the filter of the HTTP
endpoint it sends through
([#53](https://github.com/basis-no/integration-platform-adapter-camel-quarkus/issues/53));
and components other than these four. Remove the headers before such a send, with
`removeHeader("kafka.HEADERS")` and, for example,
`removeHeaders("Authorization")`. Do not remove all `kafka.*` headers: that also
removes `kafka.KEY`, which Camel's Kafka producer uses as the key of the record
the adapter parks in the DLQ.
Other record headers still reach the receiver, as byte-array text through
Camel's `http` component, unless they are `x-bip-*` headers.

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

The deployment sets `basis.message-store.deployment-replicas` from the
Kubernetes Deployment's replica count. An uncoordinated polling route is accepted only with one
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
and propagates to the caller. Payload is metadata-only, so operator tools must
not offer resend. This is an implemented Camel-only preview, not part of the
Kafka lifecycle shared with the .NET adapter, and cannot be mixed with
adapter-managed Kafka boundaries in the current global-policy implementation.
