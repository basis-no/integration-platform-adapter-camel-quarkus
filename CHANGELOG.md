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
- Every `x-bip-*` header of a Kafka record, such as `x-bip-message-id`, is kept
  as UTF-8 text before the route runs, not only the correlation id. HTTP
  receivers get the ids as text instead of byte-array text such as
  `[55, 101, …]`. Same rule as adapter 0.3.6.
- An application with more than one `@Default` JDBC DataSource, for example a
  second DataSource from a `@Named` producer, no longer starts with the Postgres
  Message Store or step store, and the error names the ambiguity. Before, it
  started, reported ready and failed every message. Same rule as adapter 0.3.6.
- A tracked send through Camel's `http` component no longer forwards
  credential-like headers, such as `Authorization` or API keys, that still hold
  a consumed Kafka record's bytes; it logs a warning once per step and header
  name, without the value. A `toD` that chooses its component only when it
  sends, such as `${header.receiverUrl}` or `language:header:receiverUrl`, is
  treated as an HTTP send. Same rule as adapter 0.3.6.
- Document the HTTP client's own 503/429 retry inside one Camel attempt, the
  Camel 4 HTTP timeout options (`socketTimeout` is not one), and what a replay
  does after a timed-out call whose effect happened.
- A replay resets an inline payload's expiry, capped at first receipt plus twice
  the retention, instead of keeping the first expiry. First receipt is the
  earliest outcome this integration recorded for the message. Object payloads
  keep their first expiry together with their object. Same rule as adapter 0.3.6,
  with the same limit: recording the same attempt again replaces its outcome
  time, so if it was the earliest, a later replay's cap starts from the next
  earliest outcome.
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
