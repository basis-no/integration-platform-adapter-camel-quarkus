# Camel Quarkus Adapter

Camel Quarkus runtime adapter for message auditing, Kafka recovery and controlled replay.
It keeps message-store persistence, retry and DLQ behavior, Kafka manual
commit, replay, and replay-aware side-effect tracking out of business routes.

The adapter is self-contained: the documentation in [`docs/`](docs/index.md)
describes the tables, topics and headers it uses, and the schema it expects
ships in `src/main/resources/db/message-store/`. With the Postgres sink it
checks at startup that `bip_contract` records store schema version 1, and it
refuses to start against a store that lacks a table or column it writes.

## Installation

Releases are available from Maven Central:

```xml
<dependency>
  <groupId>no.basis.integrationplatform</groupId>
  <artifactId>integration-platform-adapter-camel-quarkus</artifactId>
  <version>0.3.6</version>
</dependency>
```

Pin the adapter version in the integration's build and let the deployment
supply production runtime configuration. Integration repositories should not
copy message-store routes, database writes, Kafka commit processors, or DLQ
plumbing into business YAML.

## What It Provides

- selected business-message boundaries with terminal lifecycle recording;
- Kafka manual commit only after required durable outcome work succeeds;
- standard retry and terminal DLQ handling;
- stable `x-bip-*` envelope and idempotency headers;
- durable delivered-step tracking and controlled replay;
- Postgres-backed audit and message read models for operator tools;
- inline or S3-compatible replay payload storage;
- safe normalization of file, polling, and acknowledged queues into Kafka;
- guarded local test profiles;
- Kafka activity counters through the application's Micrometer registry;
- a Key-Value Store for small integration-scoped JSON cursors and markers.

The guarantee is controlled **at-least-once** processing, not distributed
exactly-once delivery across Kafka, Postgres, and external systems. See
[Steps and idempotency](docs/steps-and-idempotency.md) for the remaining crash
window and receiver responsibilities.

## Documentation

Start with the [documentation index](docs/index.md).

| Need | Document |
| --- | --- |
| Add the dependency and write a first route | [Getting started](docs/getting-started.md) |
| Understand attempts, statuses, commits, and persistence | [Message lifecycle](docs/message-lifecycle.md) |
| Classify endpoint sends and prevent duplicate effects | [Steps and idempotency](docs/steps-and-idempotency.md) |
| Understand retry, DLQ, resend, and redo | [Retry, DLQ, and replay](docs/retry-dlq-replay.md) |
| Use Kafka, files, polls, JMS, HTTP, or multiple replicas | [Ingress and scaling](docs/ingress-and-scaling.md) |
| Configure Postgres-inline and S3 payload storage | [Payload storage](docs/payload-storage.md) |
| Run local tests or deliberately suppress commits | [Testing](docs/testing.md) |
| Count Kafka admissions and publications for activity alerts | [Kafka activity metrics](docs/activity-metrics.md) |
| Keep polling cursors and markers between runs and replicas | [Key-Value Store](docs/key-value-store.md) |
| Publish a snapshot for remote acceptance or make a final release | [Releasing](docs/releasing.md) |
| Find every property, default, environment variable, and safety note | [Configuration reference](docs/configuration.md) |
| Diagnose startup, replay, persistence, or consumer problems | [Troubleshooting](docs/troubleshooting.md) |
| Upgrade an integration from 0.3.x to the unreleased 0.4.0 | [Upgrading from 0.3.x to 0.4.0](docs/upgrading-to-0.4.md) |

## Current Status

- Released versions are on the `0.3` line and use message-store schema
  version 1; see [CHANGELOG.md](CHANGELOG.md).
- Security: adapter 0.3.1 to 0.3.6 and 0.4.0-rc1 can forward a consumed
  Kafka record's credentials to an HTTP receiver, through Camel's
  `kafka.HEADERS` on camel-kafka versions before 4.14.8, 4.18.3 and 4.21.0.
  The record's credential headers themselves reach the receiver too: on
  0.3.5 and earlier and 0.4.0-rc1 through every HTTP send, and on 0.3.6
  through untracked sends such as `enrich` and through `vertx-http`. See
  [GHSA-9xmf-gp9j-qw7w](https://github.com/basis-no/integration-platform-adapter-camel-quarkus/security/advisories/GHSA-9xmf-gp9j-qw7w)
  for the workaround. Fixed for 0.4.0.
- 0.4.0-rc1 restores an output checkpoint from object storage as a stream
  that is checked only when the route reads it to its end. Fixed for 0.4.0;
  see [Output-Producing Side Effects](docs/steps-and-idempotency.md#output-producing-side-effects).
  An input in object storage is still checked that way
  ([#60](https://github.com/basis-no/integration-platform-adapter-camel-quarkus/issues/60)).
- 0.4.0-rc1 replays any stored message, also another integration's, to the
  replay topic named by its row, and does not check that the route that
  recorded it still consumes that topic. The 0.3.x releases refuse both.
  Fixed for 0.4.0; see [Manual Replay](docs/retry-dlq-replay.md#manual-replay).
- 0.3.x and 0.4.0-rc1 let Camel warn at route start that the adapter's
  intercept strategy "does not return an AsyncProcessor instance", once per
  untracked processor such as `validate`. Harmless; fixed for 0.4.0. See
  [Troubleshooting](docs/troubleshooting.md#route-fails-at-startup).
- On `main`, a replay request with `redoSteps` is refused unless delivered
  steps are recorded (the `postgres` step store); see
  [Redo Steps](docs/retry-dlq-replay.md#redo-steps).
- 0.4.0-rc1 gives an application's own `onException` handler the consumed
  input instead of the message the route left, and logs that a handled message
  is being parked. 0.3.6 fixed it, and so does 0.4.0; see
  [Terminal Kafka Failure](docs/retry-dlq-replay.md#terminal-kafka-failure).
- On `main`, a Kafka record whose outcome could not be made durable is read
  again with a growing wait instead of several times a second; see
  [Reading a record again](docs/retry-dlq-replay.md#reading-a-record-again).
- 0.3.x and 0.4.0-rc1 leave a Kafka boundary's consumer on Camel's default
  `auto.offset.reset=latest`. A record whose outcome was not durable, on a
  partition where the consumer group has no committed offset yet, is skipped
  after Camel's reconnect instead of read again. Workaround: set
  `camel.component.kafka.auto-offset-reset=earliest`. Fixed for 0.4.0, which
  ships `earliest` and refuses to start a Kafka boundary with another value;
  see [Configuration](docs/configuration.md#required-runtime-configuration).
- On `main`, only a managed Kafka input is parked in the DLQ, as in 0.3.6; a
  request on an exchange of its own, or a timer or poller input, fails back to
  its caller. See [Terminal Kafka Failure](docs/retry-dlq-replay.md#terminal-kafka-failure).
- On `main`, the failure handling follows the input type: a Kafka boundary
  parks in the DLQ, an HTTP audit-only boundary records `FAILED`, answers its
  caller and is never replayable, and the two can run in one application; see
  [Failure and Replay per Input Type](docs/retry-dlq-replay.md#failure-and-replay-per-input-type).
  A `jms:queue:` input needs `acknowledgementModeName=CLIENT_ACKNOWLEDGE`.
- `main` builds and tests against Quarkus 3.33.4 (Camel 4.18.4). The 0.3.x
  releases are built against Quarkus 3.32.2 (Camel 4.18.0).
- The 0.3 line continues as a maintained series, with its current scope (one
  input topic per integration); further 0.3.x patch releases may follow, also
  after 0.4.0. 0.3.x documents its own known
  limitations, among them what happens during a Message Store outage, on its
  [`release/0.3` branch](https://github.com/basis-no/integration-platform-adapter-camel-quarkus/blob/release/0.3/README.md#retry-and-dlq).
- Known limitation of 0.3.x: a failed send in a `direct:` sub-route is
  recorded `DELIVERED` when the sub-route's dead-letter channel parks the
  message, so a resend skips it. Resend with the step in `redoSteps`; see
  [Sends in a sub-route](docs/steps-and-idempotency.md#sends-in-a-sub-route).
  Fixed for 0.4.0.
- This `0.4.0-SNAPSHOT` development stack adds safe test profiles, tiered S3
  payload storage, output-side-effect checkpoints, non-Kafka ingress
  normalization, replica-safety validation, Kafka activity counters with
  admission counting on by default (opt-in in 0.3.6), and the 0.3.6 Key-Value
  Store.
- Delivery audit and non-Kafka input inspection from 0.3.5 and 0.3.6
  (`basis.delivery-audit.routes`, `basis.input-audit.routes`) are not in this
  development line yet; an application with either set refuses to start and
  names what to use instead. See
  [Settings Removed Since 0.3.x](docs/configuration.md#settings-removed-since-03x).
- The `0.4` features have extensive local unit and Testcontainers coverage but
  are not released until the stacked changes are merged, released, and the
  remote acceptance campaign is complete.
- Direct Postgres mode is the production path. The Kafka status sink requires a
  separate lifecycle consumer to materialize the Postgres read model.

## Development

JDK 21 is required. Use the repository Maven wrapper:

```sh
./mvnw test
./mvnw -DskipTests javadoc:javadoc
```

Behavior and configuration changes must update the relevant local documentation.
Changes to shared message-store semantics must stay compatible with the .NET
adapter.

Use `X.Y.Z-SNAPSHOT` builds from Sonatype's separate snapshot repository for
remote acceptance. Create a `vX.Y.Z` tag only after local and remote acceptance
is complete; that tag publishes the immutable final artifact to Maven Central.
See [Releasing](docs/releasing.md) for the full workflow and checklist.

## License

Apache-2.0. Issues and pull requests are welcome and handled best-effort.

## Payload capture modes

With the production Postgres sink and step-outcome store configured, the Kafka
consumer, Postgres lifecycle journal, step-outcome ledger, retry/DLQ handling
and replay admin endpoint stay active independently of payload storage.
`BASIS_MESSAGE_STORE_PAYLOAD_MODE=metadata-only` processes messages but records
no replay payload: lifecycle rows remain, with
`replayable=false` and a reason. `postgres-inline` restores payload capture
for subsequent deliveries subject to the existing size and retention policy.

This is not a no-storage mode: Kafka input/DLQ records retain their payloads
under Kafka retention, and Postgres retains lifecycle and step metadata.
Switching modes neither purges earlier history nor captures messages
retroactively. Existing replayable history is evaluated per message by the
adapter; a replay processed with capture off records a metadata-only new
attempt. Earlier attempts and the replay audit remain historical evidence.
The latest lifecycle row follows the latest attempt's capture policy.

Runtime stop/start remains deployment replica state. Do not disable the sink
or step ledger to implement payload capture off: durable recording before
commit remains mandatory. Missing or failed journal persistence must still
prevent successful acknowledgement. No new adapter package or database schema
is required for these modes; published .NET 0.3.0 and Camel 0.3.1 already
implement them.

### Runtime security bindings

Operators can supply one authenticated TLS broker binding for input, DLQ, replay
and the optional Kafka status sink. Existing installations keep their plaintext
default until configured. Application persistence uses a separate named
datasource and scoped migration identity; the default datasource remains the
adapter's message store. See the [configuration reference](docs/configuration.md).

## How this project is developed

Most code and documentation here is written with AI coding agents, directed
and reviewed by Basis maintainers. Every change goes through a pull request,
human review and CI before it is merged. [`AGENTS.md`](AGENTS.md) contains the
rules the agents follow; human contributors follow the same rules.

See [CONTRIBUTING.md](CONTRIBUTING.md) for branches and pull requests.

## Release history

This repository starts from a fresh history. Versions up to 0.3.5 were
developed in an earlier private repository, so this repository has no tags or
release pages for them. Their notes are in [CHANGELOG.md](CHANGELOG.md),
together with those of 0.3.6, the first release with a tag and a release page
here.
