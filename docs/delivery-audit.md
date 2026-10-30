# Configurable delivery audit — 0.3.5

Available in the stable `0.3.5` package. Publication and runtime qualification
do not automatically change a platform's certified pins or deployed integrations.
This extension does not require the forthcoming Message Store service or a database migration.

## Input and delivery are different

The Kafka input is the payload the integration knows how to consume. An outbound
request is a transformed payload intended for a receiver. Retaining both is
useful, but replaying the outbound bytes into the input topic is unsafe.
Maintenance 0.3.4 excludes internal request routes entirely. Version 0.3.5 keeps
that replay guard and permits selected delivery requests to be stored for inspection.

## Configuration

`basis.delivery-audit.routes` (environment mapping
`BASIS_DELIVERY_AUDIT_ROUTES`) is an empty-by-default comma-separated list
of exact Camel route ids. No wildcard selection is supported. Unknown ids and
selected routes that do not consume `direct:` fail startup.

For example:

```properties
basis.delivery-audit.routes=deliver-order,deliver-invoice
```

The integration owns the route selection. Its `basis.delivery-audit.*` namespace
is separate from platform-owned `basis.message-store.*` capture and replay settings;
managed-source conformance permits the route selector without overriding those
settings. In a managed deployment, use an approved ordinary application setting
and map it in the source, for example:

```properties
basis.delivery-audit.routes=${DELIVERY_AUDIT_ROUTES:deliver-order}
```

An empty value disables the feature. Platform-generated Message Store settings
must not be overridden in the application. Integrations without audit routes
keep the 0.3.4 recording behavior on the same adapter version.

Call each selected route with a new `ProducerTemplate.request` exchange. Copy the
original `x-bip-correlation-id`, content type and required receiver headers, but
never copy the input message id, attempt, Kafka manual-commit handle or adapter
exchange properties. The adapter assigns a new message id for every request.
An audit route entered with an existing input identity or capture/commit state
fails before its receiver operation. Do not select ordinary same-exchange
`to("direct:...")` subroutes.

Copy only the required HTTP response body and status back to the input exchange;
retain the original input identity and attempt/commit headers. The input route
owns retry and parking; the request route should use `ip-no-retry`. A receiver
failure must propagate through shared splits to that input. Do not add a catch-all
that bypasses platform recovery.

## Stored behavior

- `bip_message_log` and `bip_message_attempt` use the existing schema.
- Each request has a distinct message id, the supplied original correlation id,
  its route id and terminal `COMPLETED` or `FAILED` outcome.
- Capture occurs before the receiver response replaces the request body.
- Payload mode, size cap and retention days still apply. Metadata-only or
  oversized payloads retain metadata rather than bytes.
- Explicit audit capture is independent of `basis.message-store.replay.enabled`.
  Retained audit bytes always have `replayable=false` and an explanatory reason.
- Audit recording never commits Kafka offsets, publishes a DLQ record or handles
  the receiver exception. A journal failure fails the request; it does not claim
  successful recording. The parent input's existing recovery policy applies.
- Existing SQL-backed dashboard searches match the correlation id. Payload
  preview/download uses the normal payload authorization and retention controls.
  Audit rows are ineligible for Kafka replay in both stored flags and server-side
  route validation. No historic delivery rows are synthesized.

The audit outcome describes the observed route result, not independently proven
business acceptance. Input replay can repeat receiver calls; fresh audit ids do
not provide transaction-level deduplication. Existing input replay settings and
whole-input replay limitations are unchanged.

For file, HTTP or scheduler inputs, use the separate
[input inspection selector](input-audit.md). These records retain inspection
without claiming the Kafka recovery contract. Do not enable delivery selection
on ordinary shared input exchanges to restore non-Kafka visibility.

## Qualification and rollout

Run the adapter suite, startup fixture, real Postgres persistence tests and the
contract-v1 Kafka/Postgres suite with audit disabled. With audit enabled, prove
sibling identities/correlation, retained outgoing bytes, receiver and journal
failure propagation, no audit offset commit/DLQ publication and replay rejection
before audit/publication. Qualify an input success/failure/success sequence and
one original-input replay in an isolated tenant before customer rollout.

The earlier delivery-only candidate's local qualification on 2026-10-06 passed the adapter and startup suites, real
Kafka/PostgreSQL tests and base-contract S1–S22 for candidate Camel and released
.NET 0.3.1, with no differences after documented normalization. Additional failure
coverage proves that an audit journal error after receiver success reaches the
original input's recovery handler: only the original payload is parked, and
input-journal failure still prevents its offset commit. A managed-build
compatibility check additionally reserves the `basis.message-store.*` namespace for platform capabilities. Route selection uses `basis.delivery-audit.routes`
and is qualified through the unchanged managed-source checker and Quarkus startup.
The final combined source passed the base-contract S1–S22 suite on 2026-10-11
with matching released .NET results, focused non-Kafka regression/startup checks,
and isolated delivery inspection and original-input recovery acceptance. Verify
the selected package and application route configuration before rollout; optional
inspection does not extend the Kafka recovery contract to other transports.
