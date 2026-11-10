# Kafka activity metrics

**Implemented locally** for 0.4. The adapter counts Kafka activity through the
application's Micrometer registry. Admission counting is on by default for an
application with a Kafka boundary; publication counting stays opt-in. In 0.3.6
both were opt-in. No route, Kafka setting or alert policy is changed.

## Choose an actual event

| Signal | Counted event | Meaning |
| --- | --- | --- |
| `record_admitted` | A Kafka record reaches a selected Message Store Kafka boundary after identity and payload admission, with topic, partition and offset metadata | Processing activity, including redelivery |
| `record_published` | A producer completes with new broker-confirmed record metadata for an explicitly allowed input topic | Kafka publication activity, including acknowledged records in a partially failed batch |

These are record counts, not unique business transactions or downstream business
completion. Timers, empty database polls, direct-route retries and non-Kafka audit
rows do not count as Kafka admissions. A successful business run that publishes
nothing is not a publication event. A replay-skipped send publishes nothing and
does not count.

## Default and selection

Without `basis.traffic.signals`, the adapter counts `record_admitted` once the
application has a selected Kafka input boundary. An application without one
exports no activity metrics and starts as usual. An empty value counts as unset.

An explicit list selects exactly its events, as in 0.3.6: a list without
`record_admitted` does not count admissions. To count both, list both and the
producer topics:

```properties
basis.traffic.signals=record_admitted,record_published
basis.traffic.input-topics=example-input
```

`basis.traffic.signals=none` turns every signal off. `none` together with a
signal name is an invalid selection, which leaves every signal unsupported.

The comma-separated topic list matches exact destination topics, including other
integrations' input topics. Wildcards are unsupported.

Publication requires resolved static Camel Kafka producer endpoints with
`recordMetadata=true` and `requestRequiredAcks=all`, `-1` or `1`. The exporter
validates these settings; it does not enable metadata or change broker ACK policy.
Producer `additionalProperties.acks` overrides are checked too. Transactional
producers are unsupported: an individual record ACK does not prove transaction
commit or consumer visibility. All observed Kafka producers must provide reliable metadata, even when their usual
destination is outside the allowlist, because per-record topic overrides are
possible. Dynamic Kafka destinations, unknown dynamic schemes (a `toD` whose
component is chosen only when it sends, such as `${header.receiverUrl}` or
`language:header:receiverUrl`), ACK0 or disabled metadata make publication
coverage unsupported for that process. A successful send without fresh
assigned-offset metadata also invalidates coverage.

Only `to` and `toD` sends are observed. A Kafka publish through `enrich`,
`wireTap`, `recipientList`, `routingSlip` or a `ProducerTemplate` is neither
counted nor makes coverage unsupported, so keep publishes to the listed topics
in `to` or `toD` when `record_published` is selected.

The exporter snapshots existing metadata identities before each send and counts
only new, unique records after producer completion. It preserves application
headers and payloads. An attempted send, reused metadata or duplicate completion
callback does not add another publication. Failed sends without ACK metadata add
nothing; valid ACKs from a partially failed batch still count. Null or unassigned
metadata for failed batch elements is not an ACK and adds nothing.

## Read counters with their support and process identity

The adapter uses the application's existing Micrometer registry and Prometheus
endpoint. It adds no metrics server or framework. Without a resolvable registry,
the telemetry is unsupported and message processing continues.

| Prometheus metric | Labels | Value |
| --- | --- | --- |
| `integration_traffic_events_total` | `signal=record_admitted` or `record_published` | Monotonic per-process count while supported |
| `integration_traffic_signal_supported` | Same fixed `signal` values | `1` only for a selected (by default or explicitly) and supported event; otherwise `0` |
| `integration_traffic_process_start_time_seconds` | None | JVM process-start hint in seconds, not last activity time |

No topic, payload, message, integration or customer identifiers are metric labels.
An unselected signal's support0 does not invalidate a different selected signal.
With `none`, or by default without a Kafka boundary, no metrics are exported. Invalid configuration, missing registry,
missing boundary, unsafe producer configuration, missing metadata or integer
precision exhaustion mean **unknown**, never evidence of zero traffic. An invalid
signal remains unsupported until the process restarts with valid configuration.
Per-process counters stop being supported before exceeding `9007199254740991`
(`2^53-1`), preserving exact values in Prometheus numeric samples.

A collector must bind integration/environment from authenticated deployment
ownership, gather all participating replicas, and validate selected support,
freshness and exact nonnegative integer samples. Its process-set identity must
include actual pod and container incarnation; JVM start time alone is not unique.
Replacement, counter decrease, missing replica, stale scrape, mixed adapter
versions or a coverage gap requires a fresh complete active observation window
before evaluating no traffic. Adapters without these counters, including 0.3.5
and earlier, remain unknown for these metrics. This telemetry adds no
resend, manual-commit, Message Store schema or application-database requirement.

See [configuration](configuration.md) for property names and environment mappings.

The adapter also exports `integration_single_active_lease{route}`, the lease
state of each polling route on this replica, which is not an activity counter;
see [Single-Active Routes](single-active.md#seeing-the-holder).
