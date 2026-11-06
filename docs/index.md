# Camel Quarkus Adapter Documentation

These pages are the Camel-specific technical reference for the adapter source
version in this repository. They are plain Markdown so they read well on
GitHub and in any static documentation site.

## Choose a Path

### Integration developer

1. [Getting started](getting-started.md)
2. [Steps and idempotency](steps-and-idempotency.md)
3. [Retry, DLQ, and replay](retry-dlq-replay.md)
4. [Testing](testing.md)

### Operator

1. [Message lifecycle](message-lifecycle.md)
2. [Ingress and scaling](ingress-and-scaling.md)
3. [Payload storage](payload-storage.md)
4. [Configuration reference](configuration.md)
5. [Kafka activity metrics](activity-metrics.md)
6. [Troubleshooting](troubleshooting.md)
7. [Releasing](releasing.md)

## Sources of Truth

These pages describe the tables, topics, headers and behavior the adapter
relies on; the schema it expects ships in
`src/main/resources/db/message-store/`. They own the adapter's behavior,
configuration, examples, local tests, and limitations. The .NET adapter uses
the same message-store tables, topics and headers.

## Version Labels

- **Released** means the behavior ships in a published version and has passed
  the release acceptance described in [Releasing](releasing.md).
- **Implemented locally** means code and local tests exist on the development
  branch, but the feature is not released yet.
- **Preview** means the behavior is intentionally narrower than the supported
  Kafka lifecycle and must be enabled explicitly.
- **Proposed** means documentation describes a future design only.

Do not label implemented behavior as released before it ships in a published
version with that acceptance evidence.
