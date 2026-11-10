# Configuration Reference

This is the canonical reference for adapter-owned `basis.message-store.*`
properties. Quarkus maps each property to the uppercase environment variable
shown below. Deployment-owned values are set by the deployment that runs the
integration, for example as environment variables, and should not be copied
into integration repositories.

| Property | Environment variable | Values / default | Ownership and safety |
| --- | --- | --- | --- |
| `basis.message-store.sink` | `BASIS_MESSAGE_STORE_SINK` | `log`, `postgres`, `kafka`; default `log` | Deployment profile. Production uses `postgres`; `kafka` requires a separate lifecycle consumer. |
| `basis.message-store.status-topic` | `BASIS_MESSAGE_STORE_STATUS_TOPIC` | topic; default `bip.message.status` | Deployment-owned. Used only by the Kafka sink. |
| `basis.message-store.step-outcome-store` | `BASIS_MESSAGE_STORE_STEP_OUTCOME_STORE` | `noop`, `postgres`; default `noop` | Deployment profile. Production replay protection requires `postgres`. |
| `basis.message-store.test-profile` | `BASIS_MESSAGE_STORE_TEST_PROFILE` | `disabled`, `no-persistence`, `no-commit`, `no-persistence-no-commit`; default `disabled` | Test-only; non-default values are unsafe in production. |
| `basis.message-store.allow-test-profiles` | `BASIS_MESSAGE_STORE_ALLOW_TEST_PROFILES` | boolean; default `false` | Explicit test guard. Production deployments should keep `false`. |
| `basis.message-store.dlq-uri` | `BASIS_MESSAGE_STORE_DLQ_URI` | A fixed, non-transactional `kafka:` URI (not `acks=0`) or the application's own `direct:` route; default `kafka:bip.message.dlq` | Deployment-owned terminal parking endpoint. The adapter records `DLQ` and commits only after this endpoint accepted the record. |
| `basis.message-store.payload.mode` | `BASIS_MESSAGE_STORE_PAYLOAD_MODE` | `tiered`, `postgres-inline`, `metadata-only`; default `postgres-inline` | Deployment profile. Production should use `tiered`. |
| `basis.message-store.payload.inline-threshold-bytes` | `BASIS_MESSAGE_STORE_PAYLOAD_INLINE_THRESHOLD_BYTES` | `0..16777216`; default `1048576` | Deployment-owned threshold; do not tune per integration. |
| `basis.message-store.payload.max-bytes` | `BASIS_MESSAGE_STORE_PAYLOAD_MAX_BYTES` | bytes; unset | Deprecated alias for the inline threshold. The new property wins. |
| `basis.message-store.payload.object-max-bytes` | `BASIS_MESSAGE_STORE_PAYLOAD_OBJECT_MAX_BYTES` | threshold..`53687091200`; default `10737418240` | Deployment policy limit for object-backed payloads. |
| `basis.message-store.payload.retention-days` | `BASIS_MESSAGE_STORE_PAYLOAD_RETENTION_DAYS` | integer `>= 0`; default `30` | Deployment retention policy. With the `postgres` sink, a replay resets an inline payload's expiry, capped at first receipt plus twice the retention; an S3 object payload keeps its first expiry. |
| `basis.message-store.payload.s3.bucket` | `BASIS_MESSAGE_STORE_PAYLOAD_S3_BUCKET` | bucket; unset | Required for `tiered`; deployment-owned. |
| `basis.message-store.payload.s3.prefix` | `BASIS_MESSAGE_STORE_PAYLOAD_S3_PREFIX` | key prefix; default `payloads` | Deployment-owned claim-check namespace. |
| `basis.message-store.payload.s3.part-size-bytes` | `BASIS_MESSAGE_STORE_PAYLOAD_S3_PART_SIZE_BYTES` | integer `>= 5242880`; default `8388608` | Deployment tuning for bounded-memory multipart upload. |
| `basis.message-store.replay.enabled` | `BASIS_MESSAGE_STORE_REPLAY_ENABLED` | boolean; default `true` | Disable for audit-only/non-replayable boundaries. |
| `basis.message-store.replay.input-topic-pattern` | `BASIS_MESSAGE_STORE_REPLAY_INPUT_TOPIC_PATTERN` | one `%s` format or fixed topic; default `bip.%s.in` | Deployment-owned replay target. The adapter assumes one canonical input per integration. |
| `basis.message-store.boundary-routes` | `BASIS_MESSAGE_STORE_BOUNDARY_ROUTES` | `auto`, `all`, exact ids; default `auto` | App may name explicit preview boundaries; `auto` selects Kafka consumers. |
| `basis.message-store.ingress.normalization-routes` | `BASIS_MESSAGE_STORE_INGRESS_NORMALIZATION_ROUTES` | exact route ids; default empty | App-owned opt-in for source-to-Kafka routes; requires Postgres step store. |
| `basis.message-store.deployment-replicas` | `BASIS_MESSAGE_STORE_DEPLOYMENT_REPLICAS` | integer `>= 1`; no library default | Deployment-owned; set from the actual Kubernetes Deployment replica count. Never set or guess it in the app. |
| `basis.message-store.failure-mode` | `BASIS_MESSAGE_STORE_FAILURE_MODE` | `dlq`, `record-failed`; default `dlq` | Kafka requires `dlq`; audit-only non-Kafka preview requires `record-failed`. |
| `basis.message-store.step-ids.emergency-allow-generated` | `BASIS_MESSAGE_STORE_STEP_IDS_EMERGENCY_ALLOW_GENERATED` | `true`, `false`; default `false` | Emergency escape hatch only. By default a `to` or `toD` send to a non-internal endpoint without an explicit id fails startup; `true` starts with a warning per send. |
| `basis.message-store.step.internal-ids` | `BASIS_MESSAGE_STORE_STEP_INTERNAL_IDS` | exact node ids; default empty | App-owned replay classification. |
| `basis.message-store.step.repeatable-ids` | `BASIS_MESSAGE_STORE_STEP_REPEATABLE_IDS` | exact node ids; default empty | App-owned replay classification for safe reads/authentication. |
| `basis.message-store.step.side-effect-ids` | `BASIS_MESSAGE_STORE_STEP_SIDE_EFFECT_IDS` | exact node ids; default empty | App-owned durable write classification. Requires stable ids. |
| `basis.message-store.step.output-side-effect-ids` | `BASIS_MESSAGE_STORE_STEP_OUTPUT_SIDE_EFFECT_IDS` | exact node ids; default empty | App-owned writes whose output must be restored on replay. |
| `basis.message-store.step.output-checkpoint.header-names` | `BASIS_MESSAGE_STORE_STEP_OUTPUT_CHECKPOINT_HEADER_NAMES` | header names; default empty; credential-like names refused at startup | App-owned allowlist; only JSON-compatible, non-sensitive metadata. See [What a Checkpoint Stores](steps-and-idempotency.md#what-a-checkpoint-stores). |
| `basis.message-store.step.output-checkpoint.property-names` | `BASIS_MESSAGE_STORE_STEP_OUTPUT_CHECKPOINT_PROPERTY_NAMES` | exchange properties; default empty | App-owned allowlist; only JSON-compatible, non-sensitive metadata. |
| `basis.message-store.key-value.sweep-interval-seconds` | `BASIS_MESSAGE_STORE_KEY_VALUE_SWEEP_INTERVAL_SECONDS` | integer seconds `1..3600`; default `3600` | Adapter cleanup cadence for expired [Key-Value Store](key-value-store.md) rows after its first use; no effect while unused. |
| `basis.traffic.signals` | `BASIS_TRAFFIC_SIGNALS` | Comma-separated `record_admitted`, `record_published`, or `none`; unset or empty selects `record_admitted` | Integration-owned event selection; see [Kafka activity metrics](activity-metrics.md). An explicit list selects exactly its events. |
| `basis.traffic.input-topics` | `BASIS_TRAFFIC_INPUT_TOPICS` | Exact comma-separated Kafka producer destination topics; no wildcards; default empty | Required for `record_published`; does not change routes or Kafka settings. |
| `basis.message-store.kafka.security-protocol` | `BASIS_MESSAGE_STORE_KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` (default), `SSL`, `SASL_SSL`, `SASL_PLAINTEXT` | Deployment-owned. Use `SASL_SSL` for authenticated encrypted broker access. Plaintext modes are for trusted local compatibility only. |
| `basis.message-store.kafka.sasl-mechanism` | `BASIS_MESSAGE_STORE_KAFKA_SASL_MECHANISM` | `SCRAM-SHA-512` (default), `SCRAM-SHA-256`, `PLAIN` | Deployment-owned; must match the broker. |
| `basis.message-store.kafka.sasl-jaas-config` | `BASIS_MESSAGE_STORE_KAFKA_SASL_JAAS_CONFIG` | unset | Secret binding; required for SASL. Never place credentials in routes or Git. |
| `basis.message-store.kafka.ssl-truststore-location` | `BASIS_MESSAGE_STORE_KAFKA_SSL_TRUSTSTORE_LOCATION` | unset | Mounted CA/truststore path. Hostname verification remains enabled. |
| `basis.message-store.kafka.ssl-truststore-type` | `BASIS_MESSAGE_STORE_KAFKA_SSL_TRUSTSTORE_TYPE` | `JKS` by default | Use `PEM` for a mounted broker CA certificate. |

## Settings Removed Since 0.3.x

This version does not read these settings of adapter 0.3.5 and 0.3.6. So that
an upgraded integration does not lose their effect without notice, the
application refuses to start when one of them, or any other name under
`basis.delivery-audit.` or `basis.input-audit.`, has a value, from any
configuration source, including its environment variable. An empty value, which
disabled the feature in 0.3.x, only logs a warning; remove it.

| 0.3.x setting | Environment variable | What 0.3.x did | Instead, in this version |
| --- | --- | --- | --- |
| `basis.delivery-audit.routes` | `BASIS_DELIVERY_AUDIT_ROUTES` | Recorded each request to the selected `direct:` routes as its own non-replayable row | Not available yet ([#93](https://github.com/basis-no/integration-platform-adapter-camel-quarkus/issues/93)). Remove the setting to start without it. |
| `basis.input-audit.routes` | `BASIS_INPUT_AUDIT_ROUTES` | Recorded each run of the selected non-Kafka input routes as a non-replayable row | For an HTTP input, the [HTTP audit-only preview](ingress-and-scaling.md#http-audit-only-preview) (`basis.message-store.boundary-routes=<route id>`, `failure-mode=record-failed`, `replay.enabled=false`), which cannot be combined with adapter-managed Kafka boundaries in the same application. For a file, timer or poller input, normalize it to Kafka with `basis.message-store.ingress.normalization-routes`. Inspection of other non-Kafka inputs without Kafka is not available yet ([#93](https://github.com/basis-no/integration-platform-adapter-camel-quarkus/issues/93)). |

## Required Runtime Configuration

These are integration/runtime dependencies rather than adapter-owned options:

| Property / environment | Purpose |
| --- | --- |
| `app.name` / `APP_NAME` | Stable integration identity used in message records and ingress identity. |
| `kafka.bootstrap.servers` / `KAFKA_BOOTSTRAP_SERVERS` | Required for Kafka boundaries, replay, DLQ, and Kafka sink. |
| `camel.component.kafka.auto-commit-enable=false` | Adapter default; must remain false. |
| `camel.component.kafka.allow-manual-commit=true` | Adapter default; must remain true. |
| `camel.component.kafka.break-on-first-error=true` | Adapter default; must remain true. A record that was not committed is read again before the partition's later records, whose commit would otherwise move past it. |
| `camel.component.kafka.auto-offset-reset=earliest` | Adapter default; must remain `earliest` for every Kafka boundary, on the component, the endpoint (`autoOffsetReset`) and `additionalProperties.auto.offset.reset`. The reconnect after an uncommitted record resumes from the committed offset; on a partition where the group has none yet, `latest` would resume at the end and skip the record, and `none` stops the consumer. A Kafka boundary route with any other effective value refuses to start. A new consumer group therefore starts at the beginning of each partition's retained records; so does a group whose committed offsets expired on the broker, and a boundary without a `groupId`, for which Camel generates a new group on every start and so reprocesses every retained record on each start. Give every Kafka boundary a stable `groupId`. |
| `camel.component.http.automatic-retries-disabled` / `CAMEL_COMPONENT_HTTP_AUTOMATIC_RETRIES_DISABLED`, and the `https` one | Application-owned. Unset, the adapter sets `true` on the `http` and `https` components, so the HTTP client repeats no request by itself and every repeat is a recorded Camel redelivery, timed by `Retry-After` after a 429 or 503 (see [Automatic Retry](retry-dlq-replay.md#automatic-retry)). `false` gives the client's own retry back, uncounted and with no upper limit on its `Retry-After` wait. |
| `quarkus.datasource.jdbc.url` / `QUARKUS_DATASOURCE_JDBC_URL` | Required by Postgres sink/store/replay. |
| `quarkus.datasource.username` / `QUARKUS_DATASOURCE_USERNAME` | Message-store database identity. |
| `quarkus.datasource.password` / `QUARKUS_DATASOURCE_PASSWORD` | Secret binding when password auth is used. |
| `quarkus.s3.aws.region` / `QUARKUS_S3_AWS_REGION` | Required by the Quarkus S3 client in tiered mode. |
| `quarkus.s3.endpoint-override` / `QUARKUS_S3_ENDPOINT_OVERRIDE` | Optional S3-compatible endpoint, such as local MinIO. |
| `quarkus.s3.devservices.enabled=false` | Adapter default; prevents implicit LocalStack startup. |

Credentials, encryption, endpoint policy, and workload identity belong to the
deployment's secret and runtime configuration, never Camel route YAML.

## Production Profile

A production deployment sets the equivalent of:

```properties
basis.message-store.sink=postgres
basis.message-store.step-outcome-store=postgres
basis.message-store.payload.mode=tiered
basis.message-store.replay.enabled=true
basis.message-store.failure-mode=dlq
```

The adapter library deliberately defaults to `log`, `noop`, and
`postgres-inline` so a dependency alone does not require infrastructure in a
local project.

## Separate application data

The adapter uses the default Quarkus datasource exclusively for its message
store. Business tables belong in a separately provisioned application
database with a named datasource such as `business`; select that name explicitly
in application SQL or persistence configuration. Do not point ORM/Flyway default
migrations at the Message Store datasource. Application migration credentials
belong to a separate migration execution, never a running integration.

The operator can bound the adapter pool with
`quarkus.datasource.jdbc.max-size` and enable PostgreSQL certificate verification
through the JDBC URL and mounted CA certificate. These are Quarkus/JDBC settings,
not a grant of database ownership. Network and PostgreSQL permissions enforce
the boundary even if integration code attempts another query.

Kafka security settings above apply to the Camel component (input and DLQ) and
to the adapter's direct replay/status producers. A custom application-created
Kafka client must explicitly use its approved binding too.
