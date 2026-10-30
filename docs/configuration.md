# Maintenance adapter configuration

This branch implements the 0.3.5 candidate on the 0.3.4 maintenance runtime.
It does not include 0.4 service, object-storage or new delivery-identity settings.
Existing supported defaults and Kafka/security bindings are listed in the
[README configuration reference](../README.md#configuration).

| Property | Environment mapping | Values | Default | Ownership |
| --- | --- | --- | --- | --- |
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
