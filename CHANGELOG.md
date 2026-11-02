# Changelog

## Unreleased

- A Kafka input is committed only after it completed or the DLQ endpoint
  accepted its record. Before, a failed DLQ publish was recorded as `DLQ` and
  the offset committed, so the message was acknowledged without being parked;
  the same happened when an application's `onException` left a failure
  unhandled, so the adapter did not park it. The DLQ publish now runs in the
  adapter's `bip-confirmed-dead-letter` route, and the message counts as parked
  only after it. Otherwise the adapter records `FAILED`, does not commit the
  offset and leaves the exchange failed. The new default
  `camel.component.kafka.break-on-first-error=true` makes the consumer read
  such a record again before the partition's later records, whose commit
  would otherwise move past it; this also covers a record whose outcome could
  not be journaled. Camel skips parking for an exchange that its route stopped
  before it failed; such a message is recorded `FAILED` and not committed. The partition waits until the DLQ accepts the record, and
  each attempt logs at ERROR. A failed offset commit after a recorded outcome
  is reported as such. Same rule as adapter 0.3.6.
  `basis.message-store.dlq-uri` must now name a fixed, non-transactional
  `kafka:` endpoint whose producer waits for the broker, or the application's
  own `direct:` route, which counts as parked when it completes without an
  error. An expression such as `${header.name}`, Kafka with `acks=0` or a
  transactional producer, `direct:` with `failIfNoConsumers=false` and any
  other endpoint fail startup. A Kafka DLQ now receives one record per message
  also when the parked body is a collection; before, a collection was sent as
  one record per element, and an empty one as none. The adapter now depends on
  `camel-quarkus-direct`.
  Upgrade: keep `break-on-first-error=true`. A test that adds
  `PlatformRouteConfiguration` to a hand-built `CamelContext` must also add
  `PlatformDeadLetterRoute`; Quarkus applications get both.
- Parity with adapter 0.3.6: a `to` or `toD` send to a non-internal endpoint
  without an explicit id fails startup (emergency flag `basis.message-store.step-ids.emergency-allow-generated`),
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
- When it creates a route, the adapter logs a warning for each HTTP timeout
  option that Camel 4's `http` component accepts without applying it, and names
  the replacement. The call otherwise waits for the component's timeout, three
  minutes unless configured. Checked on Camel 4.18.0 and 4.18.4:
  `socketTimeout`, a Camel 3 option, is sent to the receiver as a query
  parameter; `httpConnection.*` applies only `soTimeout` and drops other
  timeouts; and on a `toD`, an `enrich` or a `wireTap` whose URI contains
  `${…}`, Camel's optimised send also sends `responseTimeout`, `connectTimeout`,
  `connectionRequestTimeout` and `soTimeout` to the receiver. Use the
  `httpClient.` forms or `httpConnection.soTimeout` there. The warning appears
  once per route, step and option and names no URI or value. A URI in a
  `recipientList` or `routingSlip`, or one whose scheme is only known at send
  time, is not checked. Same rule as adapter 0.3.6.
- A Kafka send that published a header with a credential-like name, by the DLQ
  masking rule, logs a warning once per route and header name. The warning names
  the header, never its value, and says how to remove it. The record is
  published as before. The check runs after a successful `to` or `toD`, against
  the endpoint the send used, follows its header filter strategy and counts only
  text, byte array, `Long`, `Integer`, `Double` and `Boolean` values. An endpoint
  whose header filter strategy or header serializer is an application class, or
  whose filter holds an application collection, is not checked, nor is a send
  through a `toD` with `cacheSize=-1` to an endpoint that is not yet registered,
  which Camel creates for that send only. At most 100 header names are reported
  per route. Same rule as adapter 0.3.6.
- A replay's `x-bip-redo-steps` stays inside the integration. When an exchange
  enters a route, the adapter reads the list, keeps it for the attempt's step
  tracking and removes the header, so no send from the route carries it: `to`,
  `toD`, `enrich`, `wireTap`, `recipientList` and `routingSlip`, nor a DLQ
  record. A route can no longer read the header; a handler that restores the
  original message gets it back until the next `to` or `toD` or DLQ parking.
  Before, a receiver got the list, and a downstream integration that consumed a
  published record with the same message id sent its own steps with the same
  ids again. `x-bip-replay` and `x-bip-attempt` are still forwarded. The warning
  for a malformed list no longer logs its value. Same rule as adapter 0.3.6.
- Document the HTTP client's own 503/429 retry inside one Camel attempt, the
  Camel 4 HTTP timeout options (`socketTimeout` is not one), and what a replay
  does after a timed-out call whose effect happened. The client retries once,
  POST included, after waiting for the receiver's `Retry-After` with no upper
  limit, or one second without it; the wait counts against
  `max.poll.interval.ms`. Setting
  `camel.component.http.automatic-retries-disabled=true` is recommended for
  receivers that send a long `Retry-After` and for routes on `ip-slow-backend`.
- A send skipped because another split part, loop iteration or multicast or
  recipient list branch of the same attempt already sent its step key is
  logged at ERROR, once per skipped send, with the step, the input route, the
  kind of part and the safe forms, and no message content or header values.
  Only the log changes. The line is written only when that is certain; every
  other skip, including a skip of an earlier attempt's delivery, stays at INFO.
  See
  [Steps and idempotency](docs/steps-and-idempotency.md#splitting-before-a-side-effect-send).
  Same rule as adapter 0.3.6.
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
