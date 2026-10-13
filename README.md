# Camel Quarkus Adapter

Camel Quarkus runtime adapter for message auditing, Kafka recovery and controlled replay.
It keeps message-store persistence, retry and DLQ behavior, Kafka manual
commit, replay, and replay-aware side-effect tracking out of business routes.

The adapter implements message-store contract `v1.0.0-rc2`. It is
self-contained: the documentation in [`docs/`](docs/index.md) describes the
tables, topics and headers it uses, and the schema it expects ships in
`src/main/resources/db/message-store/`.

## Installation

The certified release is available from Maven Central:

```xml
<dependency>
  <groupId>no.basis.integrationplatform</groupId>
  <artifactId>integration-platform-adapter-camel-quarkus</artifactId>
  <version>0.3.1</version>
</dependency>
```

Platform templates pin the supported version and supply production runtime
configuration. Integration repositories should not copy message-store routes,
database writes, Kafka commit processors, or DLQ plumbing into business YAML.

## What It Provides

- selected business-message boundaries with terminal lifecycle recording;
- Kafka manual commit only after required durable outcome work succeeds;
- standard retry and terminal DLQ handling;
- stable `x-bip-*` envelope and idempotency headers;
- durable delivered-step tracking and controlled replay;
- Postgres-backed audit and dashboard read models;
- inline or S3-compatible replay payload storage;
- safe normalization of file, polling, and acknowledged queues into Kafka;
- guarded local test profiles.

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
| Publish a snapshot for remote acceptance or make a final release | [Releasing](docs/releasing.md) |
| Find every property, default, environment variable, and safety note | [Configuration reference](docs/configuration.md) |
| Diagnose startup, replay, persistence, or consumer problems | [Troubleshooting](docs/troubleshooting.md) |

## Current Status

- Camel adapter `0.3.1` is the certified/template baseline for contract
  `v1.0.0-rc2`.
- This `0.4.0-SNAPSHOT` development stack adds safe test profiles, tiered S3
  payload storage, output-side-effect checkpoints, non-Kafka ingress
  normalization, and replica-safety validation.
- The `0.4` features have extensive local unit and Testcontainers coverage but
  are not the certified platform baseline until the stacked changes are merged,
  released, and the remote acceptance campaign is complete.
- Direct Postgres mode is the production path. The Kafka status sink requires a
  separate lifecycle consumer to materialize the Postgres read model.

## Development

JDK 21 is required. Use the repository Maven wrapper:

```sh
./mvnw test
./mvnw -DskipTests javadoc:javadoc
```

Behavior and configuration changes must update the relevant local documentation.
Cross-runtime semantic changes require a coordinated contract change.

Use `X.Y.Z-SNAPSHOT` builds from Sonatype's separate snapshot repository for
remote acceptance. Create a `vX.Y.Z` tag only after local and remote acceptance
is complete. Tags never trigger publication. Production signing and snapshot
publication are suspended pending credential isolation; unsigned inspection alone
is insufficient for a release.
See [Releasing](docs/releasing.md) for the full workflow and checklist.

## License and Support

Apache-2.0. Production support is provided to platform customers through their
platform agreement; other issues and pull requests are handled best-effort.

## Managed payload capture profile

The managed runtime keeps the v1 Kafka consumer, Postgres lifecycle journal,
step-outcome ledger, retry/DLQ handling and admin adapter active independently
of payload storage. `BASIS_MESSAGE_STORE_PAYLOAD_MODE=metadata-only` processes
messages but records no replay payload: lifecycle rows remain, with
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
is required for this profile; published .NET 0.3.0 and Camel 0.3.1 already
implement the required payload modes. A coordinated managed projection release
is required; do not infer fleet adoption from this documentation change.

### Runtime security bindings

Operators can supply one authenticated TLS broker binding for input, DLQ, replay
and the optional Kafka status sink. Existing installations keep their plaintext
default until configured. Application persistence uses a separate named
datasource and scoped migration identity; the default datasource remains the
platform Message Store. See the [configuration reference](docs/configuration.md).

## How this project is developed

Most code and documentation here is written with AI coding agents, directed
and reviewed by Basis maintainers. Every change goes through a pull request,
human review and CI before it is merged. [`AGENTS.md`](AGENTS.md) contains the
rules the agents follow; human contributors follow the same rules.

See [CONTRIBUTING.md](CONTRIBUTING.md) for branches and pull requests.

## Release history

This repository starts from a fresh history. Versions up to 0.3.5 were
developed in an earlier private repository, so this repository has no tags or
release pages for them. Their notes are in [CHANGELOG.md](CHANGELOG.md).
