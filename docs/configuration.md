# Maintenance adapter configuration

This maintenance line retains the audit options introduced in 0.3.5. The local
0.3.6 candidate adds Kafka poll recovery and safety fixes; it remains unqualified.
It does not include 0.4 service, object-storage or new delivery-identity settings.
Existing supported defaults and Kafka/security bindings are listed in the
[README configuration reference](../README.md#configuration).

| Property | Environment mapping | Values | Default | Ownership |
| --- | --- | --- | --- | --- |
| `basis.message-store.key-value.sweep-interval-seconds` | `BASIS_MESSAGE_STORE_KEY_VALUE_SWEEP_INTERVAL_SECONDS` | Integer seconds `1..3600` | `3600` | Adapter cleanup cadence after first Key-Value Store use; no effect while unused |
| `basis.delivery-audit.routes` | `BASIS_DELIVERY_AUDIT_ROUTES` | Comma-separated exact `direct:` request route ids; empty disables | empty | Integration route selection, configured through an approved application setting |
| `basis.input-audit.routes` | `BASIS_INPUT_AUDIT_ROUTES` | Comma-separated exact non-Kafka input route ids; empty disables | empty | Integration input inspection, preserving source-specific recovery |

[Delivery audit](delivery-audit.md) specifies request isolation, retained-payload
limits, failure handling and replay refusal. Audit uses the existing
`basis.message-store.payload.mode`, `basis.message-store.payload.max-bytes`
and `basis.message-store.payload.retention-days` values. It does not change
input settings, Kafka authentication, generated topics or source recovery.

[Non-Kafka input inspection](input-audit.md) covers file, HTTP and poller sources.
Its independent row identity does not replace identities assigned by producers
to their emitted Kafka messages. Input and delivery selections cannot overlap.

[Key-Value Store](key-value-store.md) covers the optional cursor and marker API,
its independent transactions, provisioning and isolation limits. Application
databases remain supported and need no migration to adopt this adapter.

## Optional Kafka activity metrics

| Property | Environment mapping | Values | Default | Ownership |
| --- | --- | --- | --- | --- |
| `basis.traffic.signals` | `BASIS_TRAFFIC_SIGNALS` | Comma-separated `record_admitted`, `record_published`; empty disables | empty | Explicit integration event selection; candidate only |
| `basis.traffic.input-topics` | `BASIS_TRAFFIC_INPUT_TOPICS` | Exact comma-separated Kafka producer destination input topics; no wildcards | empty | Required for publication export; does not change routes or Kafka settings |

[Activity metrics](activity-metrics.md) defines admission and broker-ACK semantics,
static-producer requirements, fixed metrics and unknown coverage. Empty selection
preserves existing behavior. Export alone does not enable a platform alert policy.
