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
| `basis.message-store.statement-timeout` | `BASIS_MESSAGE_STORE_STATEMENT_TIMEOUT` | duration of at least `1ms`, such as `10s`; default `10s` | Deployment-owned. Longest each statement of an outcome or step-outcome call may run (Postgres `statement_timeout`, set for that call's transaction only; inside an application transaction, the statement's JDBC query timeout). The call's JDBC network timeout is this value plus 5 seconds. Keep it well under the Kafka `max.poll.interval.ms`. See [Message Store call timeouts](#message-store-call-timeouts). |
| `basis.message-store.lock-timeout` | `BASIS_MESSAGE_STORE_LOCK_TIMEOUT` | duration of at least `1ms` and at most the statement timeout; default `5s` | Deployment-owned. Longest each such statement may wait for a lock (Postgres `lock_timeout`, for that call's transaction only; not set inside an application transaction). |
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
| `basis.message-store.step.output-checkpoint.header-names` | `BASIS_MESSAGE_STORE_STEP_OUTPUT_CHECKPOINT_HEADER_NAMES` | header names; default empty | App-owned allowlist; only JSON-compatible, non-sensitive metadata. |
| `basis.message-store.step.output-checkpoint.property-names` | `BASIS_MESSAGE_STORE_STEP_OUTPUT_CHECKPOINT_PROPERTY_NAMES` | exchange properties; default empty | App-owned allowlist; only JSON-compatible, non-sensitive metadata. |
| `basis.message-store.key-value.sweep-interval-seconds` | `BASIS_MESSAGE_STORE_KEY_VALUE_SWEEP_INTERVAL_SECONDS` | integer seconds `1..3600`; default `3600` | Adapter cleanup cadence for expired [Key-Value Store](key-value-store.md) rows after its first use; no effect while unused. |
| `basis.traffic.signals` | `BASIS_TRAFFIC_SIGNALS` | Comma-separated `record_admitted`, `record_published`, or `none`; unset or empty selects `record_admitted` | Integration-owned event selection; see [Kafka activity metrics](activity-metrics.md). An explicit list selects exactly its events. |
| `basis.traffic.input-topics` | `BASIS_TRAFFIC_INPUT_TOPICS` | Exact comma-separated Kafka producer destination topics; no wildcards; default empty | Required for `record_published`; does not change routes or Kafka settings. |
| `basis.message-store.kafka.security-protocol` | `BASIS_MESSAGE_STORE_KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` (default), `SSL`, `SASL_SSL`, `SASL_PLAINTEXT` | Deployment-owned. Use `SASL_SSL` for authenticated encrypted broker access. Plaintext modes are for trusted local compatibility only. |
| `basis.message-store.kafka.sasl-mechanism` | `BASIS_MESSAGE_STORE_KAFKA_SASL_MECHANISM` | `SCRAM-SHA-512` (default), `SCRAM-SHA-256`, `PLAIN` | Deployment-owned; must match the broker. |
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
| `camel.component.kafka.break-on-first-error=true` | Adapter default; must remain true. A record that was not committed is read again before the partition's later records, whose commit would otherwise move past it. |
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

## Message Store call timeouts

The Postgres sink and the Postgres step outcome store run on the Kafka
consumer thread. Each of their calls (recording an attempt's outcome, checking
or recording a step outcome, reading an output checkpoint) is bounded per
statement:

- On a connection in autocommit mode, the usual case, the call runs in its own
  transaction with `statement_timeout` = `basis.message-store.statement-timeout`
  (default 10 s) and `lock_timeout` = `basis.message-store.lock-timeout`
  (default 5 s), set with `set_config(..., true)` for that transaction only, so
  a pooled connection keeps no session setting.
- Each statement also has a JDBC query timeout of the statement timeout,
  rounded up to whole seconds.
- The JDBC network timeout (the PostgreSQL driver's socket timeout) is set to
  the statement timeout plus 5 seconds for the call and restored afterwards, so
  a database that stops answering without closing the connection ends the call
  too.

The timeouts bound each statement, not the call as a whole. At the defaults a
step-outcome call runs `set_config` and one statement, and ends within about
10 seconds, or 15 seconds when the database stops answering. The sink's record
runs `set_config` and up to four statements, so in the worst case, each
statement just under its timeout, it takes about 40 seconds, still well under
the Kafka `max.poll.interval.ms`. A database that stops answering ends the
call at the first statement that waits for it: the network timeout breaks the
connection.

A call that reaches a timeout fails like any other Message Store failure: the
attempt is not durable, so its Kafka offset is not committed and the record is
read again (see [Message Lifecycle](message-lifecycle.md)). Startup refuses a
timeout under 1 ms, a lock timeout longer than the statement timeout, or a
statement timeout whose network timeout does not fit the JDBC API (about 24
days).

**Inside an application transaction.** When the connection is already in a
transaction on entry (autocommit off, as on a connection enlisted in a JTA
transaction of a `transacted()` route), the transaction belongs to the
application. The step outcome store then runs its statements in that
transaction and neither begins, commits nor rolls it back, as before these
timeouts. The sink commits or rolls back the connection's transaction, as it
did before; on a connection enlisted in a JTA transaction the pool refuses that
commit, so the sink is not supported inside a JTA transaction: its record fails
as a store failure. The sink normally runs when the exchange completes, after
the route's transaction has ended. In both cases no `statement_timeout` or `lock_timeout` is set,
because a transaction-local setting would last for the rest of the
application's transaction: each statement is bounded by its JDBC query timeout
and the network timeout, and a lock wait by the query timeout.

Getting a connection is bounded by the deployment's datasource settings, which
the adapter does not set:

| Setting | Default | Recommendation |
| --- | --- | --- |
| `quarkus.datasource.jdbc.acquisition-timeout` | 5 seconds (Quarkus) | Keep it at a few seconds. It bounds the wait for a pooled connection, including one that is still being opened. |
| `connectTimeout` in the JDBC URL | 10 seconds (PostgreSQL driver) | Keep it. |
| `loginTimeout` in the JDBC URL | none (PostgreSQL driver) | Set it, for example `?loginTimeout=10`, so that opening a connection to a database that accepts the connection but never completes the login ends. |

Keep every one of these, together with the route's own retries and receiver
timeouts, well under the Kafka `max.poll.interval.ms` (300 seconds by
default). The replay service, the startup checks and the Key-Value Store are
not covered by these timeouts.

Kafka security settings above apply to the Camel component (input and DLQ) and
to the adapter's direct replay/status producers. A custom application-created
Kafka client must explicitly use its approved binding too.
