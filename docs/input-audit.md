# Non-Kafka input inspection — 0.3.5

`basis.input-audit.routes` (environment mapping `BASIS_INPUT_AUDIT_ROUTES`) selects
exact application route ids for input inspection. It defaults to empty. For
example, an integration can expose an optional ordinary application setting:

```properties
basis.input-audit.routes=${INPUT_AUDIT_ROUTES:receive-file,receive-http}
```

Unknown ids, wildcards, Kafka inputs, the platform parking route, and overlap
with `basis.delivery-audit.routes` are rejected. An explicitly empty value
disables inspection. Keep platform-generated `basis.message-store.*` settings
under platform ownership.

Select the actual non-Kafka input boundary: for example an SFTP consumer, HTTP
ingress, scheduler, or a dedicated fresh `direct:` input called by a poller.
Do not select a shared child of an already captured input. Inherited replay,
capture or Kafka commit state is rejected before processing. Keep each source's
existing acknowledgement, retry, file move, HTTP error response or lock handling.
Inspection does not add a recovery mechanism for those protocols.

Each inspection gets a fresh row id. Operational message and correlation headers
remain unchanged, including when they are absent. A poll or batch therefore does
not seed one generated identity into all its emitted Kafka records. The stored
row retains supplied correlation; otherwise it uses the incoming message id, or
its own row id when neither is supplied. The row snapshots input
headers and safely capturable bytes before the route transforms them or returns
a response. Recording does not overwrite the producer's completion headers.

Every row has `replayable=false`: retained bytes are available for inspection,
never for sending into a Kafka input. Payload mode, inline size cap and retention
still apply, independently of Kafka replay enablement. Streams and file wrappers
are metadata-only; inspection does not consume them to manufacture a payload.
Timer inspection describes a poll invocation, not each subsequently fetched
business record. Select a dedicated fresh per-record boundary when that is the
required inspection granularity.

The row describes the observed Camel route outcome. Source-specific handlers
may intentionally handle failures; inspection does not redefine business
completion or prove source acknowledgement. A Message Store write failure marks
the exchange failed without swallowing an existing failure. Inspection neither
commits Kafka offsets nor parks a DLQ record. There is no schema migration or
history backfill.

Before a fleet upgrade, review actual adapter dependencies, route ids, source
recovery and header handling. Applications that deliberately have no adapter
are not missing a version pin. Installing one changes their runtime and recovery
ownership and requires a separate migration.
