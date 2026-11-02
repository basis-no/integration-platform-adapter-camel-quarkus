# Changelog

## Unreleased

- A replay resets an inline payload's expiry, capped at first receipt plus twice
  the retention, instead of keeping the first expiry. First receipt is the
  earliest outcome this integration recorded for the message. Object payloads
  keep their first expiry together with their object. Same rule as adapter 0.3.6.
- Separate mutable `X.Y.Z-SNAPSHOT` remote-acceptance builds from immutable
  final `vX.Y.Z` Maven Central releases.
- Avoid a null-method classification failure for ordinary Camel HTTP producers,
  and log exhausted processing failures with their stack traces before DLQ parking.
- Honor the deprecated payload inline-limit alias when the replacement property
  is not configured, including for replay step checkpoints.
- Split the adapter guide into task-focused documentation for setup, lifecycle,
  idempotency, replay, ingress, payload storage, testing, configuration, and
  troubleshooting.
- Add automated configuration coverage, Markdown link validation, and strict
  warning-free public Javadocs.
- Describe the adapter in its own terms throughout the documentation, Javadoc,
  and the startup messages about the store schema version, a missing replica
  count and an ingress route without its route configuration. The
  `MESSAGE-STORE CONTRACT VERSION UNVERIFIED` prefix, behavior, identifiers and
  configuration are unchanged.

## 0.3.1

First public release, on Maven Central. Functionally identical to the
internally qualified 0.3.0 (message-store schema version 1); this release
adds only licensing, publication metadata and packaging. Versions below 0.3.1
were internal to Basis.
