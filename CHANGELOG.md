# Changelog

## Unreleased

- Parity with adapter 0.3.6: an endpoint send without an explicit id fails
  startup (emergency flag `basis.message-store.step-ids.emergency-allow-generated`),
  and keeping the generated id as the explicit id, such as `id: to1`, keeps the
  step key;
  Kafka poll failures reconnect; a UTF-8 Kafka correlation id stays text under
  both camel-kafka header names; credentials are masked in journaled step
  endpoints and in DLQ header values; a late older attempt cannot replace the
  newer summary; a route that drops headers is still recorded under the input's
  identity and attempt; a parked failure keeps the input's message id; skipped
  steps are logged at INFO and the logging sink counts step outcomes.
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

## 0.3.1

First public release, on Maven Central. Functionally identical to the
internally certified 0.3.0 (message-store contract v1.0.0-rc2); this release
adds only licensing, publication metadata and packaging. Versions below 0.3.1
were internal to the Basis platform.
