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

### Platform engineer

1. [Message lifecycle](message-lifecycle.md)
2. [Ingress and scaling](ingress-and-scaling.md)
3. [Payload storage](payload-storage.md)
4. [Configuration reference](configuration.md)
5. [Troubleshooting](troubleshooting.md)
6. [Releasing](releasing.md)

## Sources of Truth

The adapter implements contract `v1.0.0-rc2`. These pages describe the
tables, topics, headers and behavior the adapter relies on; the schema it
expects ships in `src/main/resources/db/message-store/`.

These pages own Camel-specific behavior, configuration, examples, local tests,
and limitations. The public platform handbook owns curated runtime-neutral
guidance, the certified adapter-version matrix, and release snapshots.

## Version Labels

- **Certified** means the version matrix and cross-runtime certification suite
  approve the behavior for platform use.
- **Implemented locally** means code and local tests exist on the development
  branch, but the feature is not yet the platform baseline.
- **Preview** means the behavior is intentionally narrower than the certified
  contract and must be enabled explicitly.
- **Proposed** means documentation describes a future contract or design only.

Do not promote implemented or published behavior to certified status without
the coordinated acceptance evidence.
