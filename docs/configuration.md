# Configuration Reference

This is the canonical reference for adapter-owned `basis.message-store.*`
properties. Quarkus maps each property to the uppercase environment variable
shown below. Platform-owned values are injected by the shared chart and should
not be copied into integration repositories.

| Property | Environment variable | Values / default | Ownership and safety |
| --- | --- | --- | --- |
| `basis.message-store.sink` | `BASIS_MESSAGE_STORE_SINK` | `log`, `postgres`, `kafka`; default `log` | Platform profile. Production v1 uses `postgres`; `kafka` requires a separate lifecycle consumer. |
| `basis.message-store.status-topic` | `BASIS_MESSAGE_STORE_STATUS_TOPIC` | topic; default `bip.message.status` | Platform-owned. Used only by the Kafka sink. |
| `basis.message-store.step-outcome-store` | `BASIS_MESSAGE_STORE_STEP_OUTCOME_STORE` | `noop`, `postgres`; default `noop` | Platform profile. Production replay protection requires `postgres`. |
| `basis.message-store.test-profile` | `BASIS_MESSAGE_STORE_TEST_PROFILE` | `disabled`, `no-persistence`, `no-commit`, `no-persistence-no-commit`; default `disabled` | Test-only; non-default values are unsafe in production. |
| `basis.message-store.allow-test-profiles` | `BASIS_MESSAGE_STORE_ALLOW_TEST_PROFILES` | boolean; default `false` | Explicit test guard. Platform deployments should keep `false`. |
| `basis.message-store.dlq-uri` | `BASIS_MESSAGE_STORE_DLQ_URI` | Camel URI; default `kafka:bip.message.dlq` | Platform-owned terminal parking endpoint. |
| `basis.message-store.payload.mode` | `BASIS_MESSAGE_STORE_PAYLOAD_MODE` | `tiered`, `postgres-inline`, `metadata-only`; default `postgres-inline` | Platform profile. Production should use `tiered`. |
| `basis.message-store.payload.inline-threshold-bytes` | `BASIS_MESSAGE_STORE_PAYLOAD_INLINE_THRESHOLD_BYTES` | `0..16777216`; default `1048576` | Platform-owned threshold; do not tune per integration. |
| `basis.message-store.payload.max-bytes` | `BASIS_MESSAGE_STORE_PAYLOAD_MAX_BYTES` | bytes; unset | Deprecated alias for the inline threshold. The new property wins. |
| `basis.message-store.payload.object-max-bytes` | `BASIS_MESSAGE_STORE_PAYLOAD_OBJECT_MAX_BYTES` | threshold..`53687091200`; default `10737418240` | Platform policy limit for object-backed payloads. |
| `basis.message-store.payload.retention-days` | `BASIS_MESSAGE_STORE_PAYLOAD_RETENTION_DAYS` | integer `>= 0`; default `30` | Platform retention policy; replay does not extend expiry. |
| `basis.message-store.payload.s3.bucket` | `BASIS_MESSAGE_STORE_PAYLOAD_S3_BUCKET` | bucket; unset | Required for `tiered`; platform-owned. |
| `basis.message-store.payload.s3.prefix` | `BASIS_MESSAGE_STORE_PAYLOAD_S3_PREFIX` | key prefix; default `payloads` | Platform-owned claim-check namespace. |
| `basis.message-store.payload.s3.part-size-bytes` | `BASIS_MESSAGE_STORE_PAYLOAD_S3_PART_SIZE_BYTES` | integer `>= 5242880`; default `8388608` | Platform tuning for bounded-memory multipart upload. |
| `basis.message-store.replay.enabled` | `BASIS_MESSAGE_STORE_REPLAY_ENABLED` | boolean; default `true` | Disable for audit-only/non-replayable boundaries. |
| `basis.message-store.replay.input-topic-pattern` | `BASIS_MESSAGE_STORE_REPLAY_INPUT_TOPIC_PATTERN` | one `%s` format or fixed topic; default `bip.%s.in` | Platform-owned replay target. v1 assumes one canonical input. |
| `basis.message-store.boundary-routes` | `BASIS_MESSAGE_STORE_BOUNDARY_ROUTES` | `auto`, `all`, exact ids; default `auto` | App may name explicit preview boundaries; `auto` selects Kafka consumers. |
| `basis.message-store.ingress.normalization-routes` | `BASIS_MESSAGE_STORE_INGRESS_NORMALIZATION_ROUTES` | exact route ids; default empty | App-owned opt-in for source-to-Kafka routes; requires Postgres step store. |
| `basis.message-store.deployment-replicas` | `BASIS_MESSAGE_STORE_DEPLOYMENT_REPLICAS` | integer `>= 1`; no library default | Chart-owned Deployment count. Never set or guess it in the app. |
| `basis.message-store.failure-mode` | `BASIS_MESSAGE_STORE_FAILURE_MODE` | `dlq`, `record-failed`; default `dlq` | Kafka requires `dlq`; audit-only non-Kafka preview requires `record-failed`. |
| `basis.message-store.step.internal-ids` | `BASIS_MESSAGE_STORE_STEP_INTERNAL_IDS` | exact node ids; default empty | App-owned replay classification. |
| `basis.message-store.step.repeatable-ids` | `BASIS_MESSAGE_STORE_STEP_REPEATABLE_IDS` | exact node ids; default empty | App-owned replay classification for safe reads/authentication. |
| `basis.message-store.step.side-effect-ids` | `BASIS_MESSAGE_STORE_STEP_SIDE_EFFECT_IDS` | exact node ids; default empty | App-owned durable write classification. Requires stable ids. |
| `basis.message-store.step.output-side-effect-ids` | `BASIS_MESSAGE_STORE_STEP_OUTPUT_SIDE_EFFECT_IDS` | exact node ids; default empty | App-owned writes whose output must be restored on replay. |
| `basis.message-store.step.output-checkpoint.header-names` | `BASIS_MESSAGE_STORE_STEP_OUTPUT_CHECKPOINT_HEADER_NAMES` | header names; default empty | App-owned allowlist; only JSON-compatible, non-sensitive metadata. |
| `basis.message-store.step.output-checkpoint.property-names` | `BASIS_MESSAGE_STORE_STEP_OUTPUT_CHECKPOINT_PROPERTY_NAMES` | exchange properties; default empty | App-owned allowlist; only JSON-compatible, non-sensitive metadata. |
| `basis.message-store.kafka.security-protocol` | `BASIS_MESSAGE_STORE_KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` (default), `SSL`, `SASL_SSL`, `SASL_PLAINTEXT` | Platform-owned. Use `SASL_SSL` for authenticated encrypted broker access. Plaintext modes are for trusted local compatibility only. |
| `basis.message-store.kafka.sasl-mechanism` | `BASIS_MESSAGE_STORE_KAFKA_SASL_MECHANISM` | `SCRAM-SHA-512` (default), `SCRAM-SHA-256`, `PLAIN` | Platform-owned; must match the broker. |
| `basis.message-store.kafka.sasl-jaas-config` | `BASIS_MESSAGE_STORE_KAFKA_SASL_JAAS_CONFIG` | unset | Secret binding; required for SASL. Never place credentials in routes or Git. |
| `basis.message-store.kafka.ssl-truststore-location` | `BASIS_MESSAGE_STORE_KAFKA_SSL_TRUSTSTORE_LOCATION` | unset | Mounted CA/truststore path. Hostname verification remains enabled. |
| `basis.message-store.kafka.ssl-truststore-type` | `BASIS_MESSAGE_STORE_KAFKA_SSL_TRUSTSTORE_TYPE` | `JKS` by default | Use `PEM` for a mounted broker CA certificate. |

## Required Runtime Configuration

These are integration/runtime dependencies rather than adapter-owned options:

| Property / environment | Purpose |
| --- | --- |
| `app.name` / `APP_NAME` | Stable integration identity used in message records and ingress identity. |
| `kafka.bootstrap.servers` / `KAFKA_BOOTSTRAP_SERVERS` | Required for Kafka boundaries, replay, DLQ, and Kafka sink. |
| `camel.component.kafka.auto-commit-enable=false` | Adapter default; must remain false. |
| `camel.component.kafka.allow-manual-commit=true` | Adapter default; must remain true. |
| `quarkus.datasource.jdbc.url` / `QUARKUS_DATASOURCE_JDBC_URL` | Required by Postgres sink/store/replay. |
| `quarkus.datasource.username` / `QUARKUS_DATASOURCE_USERNAME` | Platform database identity. |
| `quarkus.datasource.password` / `QUARKUS_DATASOURCE_PASSWORD` | Platform secret binding when password auth is used. |
| `quarkus.s3.aws.region` / `QUARKUS_S3_AWS_REGION` | Required by the Quarkus S3 client in tiered mode. |
| `quarkus.s3.endpoint-override` / `QUARKUS_S3_ENDPOINT_OVERRIDE` | Optional S3-compatible endpoint, such as local MinIO. |
| `quarkus.s3.devservices.enabled=false` | Adapter default; prevents implicit LocalStack startup. |

Credentials, encryption, endpoint policy, and workload identity belong to the
platform secret/runtime configuration, never Camel route YAML.

## Production Profile

The shared chart supplies the equivalent of:

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

The adapter uses the default Quarkus datasource exclusively for the platform
Message Store. Business tables belong in a separately provisioned application
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
