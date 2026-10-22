# Changelog

## 0.3.6 — Unreleased

- Preserve UTF-8 Kafka correlation IDs as text before downstream processing, so
  HTTP forwarding matches journal identity instead of emitting byte-array text.
  Other header types and binary application headers are unchanged.

### Added

- Add opt-in Kafka record admission and broker-ACK publication counters via the
  existing Micrometer registry. Defaults are disabled. Missing, unsafe or
  incomplete coverage is unknown; counters do not claim business completion.
  Publication requires explicit destination topics and safe static producer
  metadata/ACK configuration. No route or Kafka setting changes automatically.

- Prepare bounded synthetic PostgreSQL qualification fixtures with exact run
  ownership and partial-startup cleanup. Certification and final combined-candidate execution remain release gates.

- Versions are scoped to a physical key row. Unswept expiry preserves its
  counter; expiry cleanup or explicit deletion followed by recreation starts
  at 1. A stale numeric CAS can match the new row. This is a candidate
  limitation under the unlocked draft contract, subject to final release review, not lease or fencing
  support. Durable cursor migration keeps no TTL, no deletion and existing
  polling protection.

- An integration-scoped `KeyValueStore` API for small JSON cursors and markers,
  with versions, compare-and-set, optional expiry, bounded cleanup and a Camel
  idempotent repository. SQL statements have a five-second execution bound;
  sweeper disposal waits up to ten seconds and reports incomplete shutdown. It uses the platform-provisioned `bip_key_value` table
  from draft contract `1.2.0-rc2`; it does not create or migrate tables.
- This is a local candidate, not a published or certified adapter pin. It
  preserves the complete maintenance baseline and does not add leases or the
  Trusted Message Store. Release remains subject to review. Existing application databases remain supported; using the API or migrating data is optional.


### Security and compatibility

- Unsigned candidate builds receive no production signing/publishing credentials.
  Signed candidate and snapshot publication remain suspended until an enforceable
  isolated signer/publisher path and credential review are complete.
- Existing application databases and named datasources remain supported. No
  migration or Message Store schema change is required while KV is unused.

### Fixed

- A Kafka poll failure reconnects and polls the same position again
  (`camel.component.kafka.poll-on-error=RECONNECT`). Before, a missing read
  permission journaled a `FAILED` row without a record and stopped the consumer
  for good while the process stayed up; another poll failure moved past the
  offset. An authentication failure still stops the consumer.

- The endpoint URI journaled for each step masks credentials the way Camel
  masks them in its logs. Before, a fixed `to` endpoint with a password in its
  user info or in an option such as `password` or `accessToken` stored that
  password in the Message Store (`bip_step_outcome.endpoint_uri`). Step ids
  still derive from the exact URI, so existing step identities and the
  delivered-step guard are unchanged. Rows journaled before the upgrade are
  not rewritten. The missing-node-id warning also masks the endpoint; arbitrary
  exception text and application logging remain outside this guarantee.

- A late outcome from an older attempt still records that attempt's history,
  but cannot replace a newer attempt's current message summary, including its
  status, payload, error and timestamps. Same-attempt updates remain supported.

### Known limitations

- Poll-time authorization failures can recover after a permission grant, but
  invalid authentication remains non-recoverable. A SmallRye-only health endpoint
  can remain Ready after polling stops. The upstream Camel health bridge passed
  the pinned maintenance fixture; it remains an application composition
  prerequisite and is not automatically added by this adapter. Qualify each
  application's actual BOM and route lifecycle. Reconnecting readiness can
  fluctuate; the finite stopped-poller proof is not a continuous permission or
  business-acceptance guarantee.
  See [maintenance qualification](docs/maintenance-qualification.md).

## 0.3.5 — 2026-10-12

Maven artifact:
`no.basis.integrationplatform:integration-platform-adapter-camel-quarkus:0.3.5`.
The source version is `0.3.5-SNAPSHOT`; a snapshot is not this stable release.
The Kafka recovery baseline remains Message Store contract `v1.0.0-rc2`.
Publication, platform certification and application rollout are separate gates.

### Added

- Opt-in `basis.delivery-audit.routes`: exact `direct:` request route ids retain
  outgoing request bytes and terminal outcomes with distinct message ids and the
  supplied original correlation id. The default empty selection preserves
  0.3.4 recording behavior. See [delivery audit](docs/delivery-audit.md).
- Opt-in `basis.input-audit.routes`: inspect selected non-Kafka input boundaries
  without assigning Kafka replay, offset commit or DLQ ownership. Input headers
  remain unchanged, including absent ids and poll/batch producer identities.
  See [input inspection](docs/input-audit.md).

### Fixed and preserved

- Receiver and audit-journal failures propagate to the original input's recovery
  handler. Recording an audit never claims successful delivery or commits an
  input offset; failed durable input recording still prevents offset commit.
- Audit payloads are always non-replayable. Delivery audit cannot resend
  transformed receiver bytes into the Kafka input, even when those bytes are
  retained or the platform enables replay.
- Route selectors are separate from platform-owned `basis.message-store.*`
  settings. Startup rejects unknown, wildcard, overlapping or unsafe inherited
  identity/capture/commit selections before processing.

### Upgrade actions and limitations

- No Message Store schema migration or new service is required. Existing
  integrations may use the same version with both selectors empty.
- Review route ids, source acknowledgement/recovery and header handling before
  opting in. Each delivery audit needs a fresh `ProducerTemplate.request`
  exchange carrying only the required correlation/content/receiver headers.
  A same-exchange `to("direct:...")` child is not a delivery audit boundary.
- Non-Kafka input inspection retains visibility only; it does not implement
  source-specific retry, acknowledgement or recovery. Streams/file wrappers
  remain metadata-only. Poll inspection is not per-record batch inspection.
- Replay the original input to recover a delivery. This may repeat external
  calls; audit ids are not business-level deduplication. Route success does not
  independently prove receiver business acceptance.
- Existing payload mode, inline size cap and retention still apply. There is no
  historical row backfill. Applications with no adapter need a separate
  integration decision, not an automatic dependency insertion.
- Dashboard searches must support correlation ids. Operational failure counts
  must distinguish delivery audit history from source outcomes; adapter upgrade
  alone does not change an older dashboard's counting behavior.

See the [configuration reference](docs/configuration.md) for defaults and
[maintenance qualification](docs/maintenance-qualification.md) for the Kafka
baseline and release provenance requirements.

## 0.3.4

- Restrict automatic Message Store capture/replay boundaries to Kafka inputs;
  internal `direct:` and HTTP receiver exchanges no longer become replay inputs.
- Keep infrastructure DLQ parking outside delivered-step skipping and preserve
  original receiver failure diagnostics.
- Require confirmed durable failure parking before Kafka offset commit. Add
  startup and maintenance acceptance fixtures for these recovery boundaries.

## 0.3.3

- Apply configured Kafka TLS/SASL authentication to adapter-owned parking and
  replay producers, matching the application's Kafka security configuration.
  Hostname verification remains enabled; existing plaintext is the default.

## 0.3.2

- Exclude XSLT transformations from delivered-step tracking so replay repeats
  transformation work instead of treating it as a delivered external effect.
- Refresh the README and link the public Message Store contract.

## 0.3.1

First public release, on Maven Central. Functionally identical to the
internally certified 0.3.0 (message-store contract v1.0.0-rc2); this release
adds only licensing, publication metadata and packaging. Versions below 0.3.1
were internal to the Basis platform.
