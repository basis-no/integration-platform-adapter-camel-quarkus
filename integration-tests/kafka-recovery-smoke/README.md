# Kafka ACL recovery and readiness fixture

Both the SmallRye-only baseline and optional upstream health-bridge profile
passed isolated local qualification on Quarkus BOM 3.32.2, Camel Quarkus 3.32.0,
Camel 4.18.0 and SmallRye Health 4.3.0. This is finite fixture evidence, not a
release, template certification or customer deployment. The adapter library is
unchanged: the health bridge is an application dependency composition.

## What it proves

One synthetic loopback SASL broker withholds topic READ while granting topic
DESCRIBE and group READ. A real Camel TopicAuthorizationException must produce
no phantom lifecycle outcome and no committed offset advance. A scoped READ
grant must recover the same consumer, process the supplied message/correlation
IDs once, and manually commit offset 1. CaptureSink observes lifecycle calls in
memory; this does not prove SQL durability.

| Composition | Healthy idle | Non-recoverable stopped-auth poller |
| --- | --- | --- |
| SmallRye Health alone | HTTP 200 | HTTP 200: confirmed readiness gap |
| With Camel MicroProfile Health | HTTP 200 | HTTP 503, repeated three times |

The optional `camel-health-bridge` profile adds the BOM-managed
`camel-quarkus-microprofile-health` extension. It verifies the native Kafka check
`consumer:kafka-<route>` and registered `consumer:<route>` wrapper, then actual
HTTP responses. All unrelated consumers must stay healthy. Every HTTP DOWN
check must identify the observed route.id, preventing unrelated failures from
satisfying the test.

During denial/reconnect the observed states and HTTP responses fluctuate. The
passing run retained UP/UP with HTTP 200 and DOWN/DOWN with HTTP 200 before observing DOWN/DOWN with HTTP 503.
Acceptance is that bounded observation within 30 seconds, not continuously DOWN
health throughout a permission outage. After recovery, actual idle HTTP 200 is
mandatory. After causal authentication error and exit from the same polling
thread, task state must be non-ready/non-recoverable and the valid consumer must
stay healthy. Three fresh native/wrapper DOWN and HTTP 503 observations span at
least two seconds; any contrary sample fails immediately. This proves that
finite window, not indefinite stability or business acceptance.

The pinned bridge evaluates on each request. SmallRye's synchronous check path
uses a cold supplier; there is no periodic refresh in this path. One-second
spacing is this fixture's observation cadence, not an upstream refresh default.
The bounded wait returns on first true without re-evaluating a volatile
predicate. Raw state pairs, HTTP bodies and phase times remain required evidence.

## Supported wiring and fixture controls

- SmallRye Health is an actual runtime dependency. It alone does not bridge
  Camel checks to HTTP. Dependency management does not add a dependency.
- Testcontainers Kafka is an actual test dependency pinned to 2.0.3.
- The adapter's `@Produces @DefaultBean @ApplicationScoped` sink fallback is
  replaced by the ordinary CaptureSink application bean. The test asserts
  resolvable CDI selection and its type before starting either route.
- The resource supplies both `kafka.bootstrap.servers` and Camel's broker
  binding, plus supported `basis.message-store.kafka.*` synthetic authentication.
  Both endpoints assert actual broker/protocol/mechanism before polling. The
  intentional bad-auth endpoint never falls back to an absent/default broker.
- Routes initially remain stopped for phase ordering. Only in the bridge
  profile, activation sets supported Route.setAutoStartup(true). The pinned
  consumer wrapper exempts autoStartup=false without a per-route controller.
  This is fixture activation, not an adapter/template/customer default change.
- Camel 4.18's natural poll-auth exit does not set isTerminated. The observer
  requires real SASL error and exit from the same polling thread plus task state;
  a log line or internal flag alone is insufficient.

Use a bounded, supervised test window. The pinned cached Confluent 7.6.1 broker
has no pulls, mandatory unique bip.test.run-id, loopback-only ports, no host binds,
768 MiB memory=swap, one CPU and PID 256. Its admin/reader and internal anonymous
controller superuser exist only inside this synthetic fixture. Retain actual
PID/CID/session/port/volume/network ownership and positive cleanup proof. A
passing functional test does not replace host pressure/paging evidence.

A different application or template BOM needs its own dependency and runtime
qualification. No automatic adapter dependency/default, contract certification,
S1–S22 completion or existing integration rewrite is implied.

Pinned source references:

- [Health registration](https://github.com/apache/camel-quarkus/blob/3.32.0/extensions/microprofile-health/deployment/src/main/java/org/apache/camel/quarkus/component/microprofile/health/deployment/MicroProfileHealthProcessor.java)
- [Health registry](https://github.com/apache/camel-quarkus/blob/3.32.0/extensions/microprofile-health/runtime/src/main/java/org/apache/camel/quarkus/component/microprofile/health/runtime/CamelMicroProfileHealthRecorder.java)
- [Kafka reconnect strategy](https://github.com/apache/camel/blob/camel-4.18.0/components/camel-kafka/src/main/java/org/apache/camel/component/kafka/consumer/errorhandler/ReconnectErrorStrategy.java)
