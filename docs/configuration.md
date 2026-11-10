# Maintenance adapter configuration

This maintenance line retains the audit options introduced in 0.3.5. 0.3.6, the
current maintenance release, adds Kafka poll recovery and safety fixes, and the
opt-in Key-Value Store and Kafka activity counters.
It does not include 0.4 service, object-storage or new delivery-identity settings.
Existing supported defaults and Kafka/security bindings are listed in the
[README configuration reference](../README.md#configuration).

| Property | Environment mapping | Values | Default | Ownership |
| --- | --- | --- | --- | --- |
| `basis.message-store.key-value.sweep-interval-seconds` | `BASIS_MESSAGE_STORE_KEY_VALUE_SWEEP_INTERVAL_SECONDS` | Integer seconds `1..3600` | `3600` | Adapter cleanup cadence after first Key-Value Store use; no effect while unused |
| `basis.delivery-audit.routes` | `BASIS_DELIVERY_AUDIT_ROUTES` | Comma-separated exact `direct:` request route ids; empty disables | empty | Integration route selection, configured through an approved application setting |
| `basis.message-store.statement-timeout` | `BASIS_MESSAGE_STORE_STATEMENT_TIMEOUT` | Duration of at least `1ms`, such as `10s` | `10s` | Deployment-owned; from 0.3.7. Bounds each statement of a Postgres sink or Postgres step outcome store call; see [Message Store call timeouts](#message-store-call-timeouts) |
| `basis.message-store.lock-timeout` | `BASIS_MESSAGE_STORE_LOCK_TIMEOUT` | Duration of at least `1ms` and at most the statement timeout | `5s` | Deployment-owned; from 0.3.7. Bounds each statement's wait for a lock; see [Message Store call timeouts](#message-store-call-timeouts) |
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

## Message Store call timeouts

From 0.3.7 each call of the Postgres sink and the Postgres step outcome store,
which run on the Kafka consumer thread, is bounded per statement: recording an
attempt's outcome, and checking or recording a step outcome. Up to 0.3.6 such a
call waited without limit on a held lock or on a database that stopped
answering, and held the consumer thread past `max.poll.interval.ms`.

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
runs `set_config` and up to three statements, so in the worst case, each
statement just under its timeout, it takes about 30 seconds, still well under
the Kafka `max.poll.interval.ms`. A database that stops answering ends the
call at the first statement that waits for it: the network timeout breaks the
connection.

A call that reaches a timeout fails like any other Message Store failure: the
attempt is not durable, so its Kafka offset is not committed and the record is
read again (see [Retry and DLQ](../README.md#retry-and-dlq)). Startup refuses a
timeout under 1 ms, a lock timeout longer than the statement timeout, or a
statement timeout whose network timeout does not fit the JDBC API (about 24
days). Raise a value only for a store that legitimately needs longer, and keep
it well under `max.poll.interval.ms`.

**Inside an application transaction.** When the connection is already in a
transaction on entry (autocommit off, as on a connection enlisted in a JTA
transaction of a `transacted()` route), the transaction belongs to the
application. The step outcome store then runs its statements in that
transaction and neither begins, commits nor rolls it back, as before these
timeouts. The sink commits or rolls back the connection's transaction, as it
did before; on a connection enlisted in a JTA transaction the pool refuses that
commit, so the sink is not supported inside a JTA transaction: its record fails
as a store failure. The sink normally runs when the exchange completes, after
the route's transaction has ended. In both cases no `statement_timeout` or
`lock_timeout` is set, because a transaction-local setting would last for the
rest of the application's transaction: each statement is bounded by its JDBC
query timeout and the network timeout, and a lock wait by the query timeout.

**Synchronous replication.** With a synchronous standby, a commit waits until
the standby confirms it, and `statement_timeout` does not end that wait. Three
things do, and each makes the call a Message Store failure:

- The network timeout: a commit that waits longer than the statement timeout
  plus 5 seconds breaks the connection, and the call fails.
- A terminated connection (`pg_terminate_backend`, a server shutdown): the
  server sends a warning with SQLSTATE `57P01` and ends the connection, so the
  commit fails either way.
- A cancel of the wait (a client cancel, `pg_cancel_backend`). PostgreSQL then
  reports the commit as done with only a warning, "canceling wait for
  synchronous replication due to user request", detail "The transaction has
  already committed locally, but might not have been replicated to the
  standby.", so the outcome could be lost in a failover. The adapter clears the
  connection's warnings before the commit and treats any warning-level message
  the commit reports (an SQLSTATE outside class `00`; notices are not counted)
  as a failure. It does not rely on the text or on a SQLSTATE of the
  synchronous replication warning: the user-cancel warning has only the generic
  `01000`, and its text follows the server's `lc_messages`. The text is matched
  only to name the cause in the error, "committed locally, but a synchronous
  standby did not confirm it". Any other warning a commit reports also fails
  the call, with "committed, but the commit reported a warning".

The check needs the warning to reach the adapter, that is `client_min_messages`
at `warning` or lower: with `error`, set on the role, the database or in the
JDBC URL's `options`, the server does not send it and the commit counts as
confirmed. The call's own transaction therefore sets `client_min_messages` to
`warning` for that transaction only, along with the timeouts. The sink's commit
of an application's transaction cannot set it without changing the
application's transaction, so there the check depends on the session's
setting; keep `client_min_messages` at its default, `notice`, for the Message
Store login.

Such a failure is not a rollback: the outcome may already be in the store. The
record is read again and its outcome written again, which is safe, because the
writes are idempotent: the sink upserts the message row by message id and the
attempt row by message id and attempt, and the step outcome store upserts by
idempotency key and never replaces `DELIVERED`. Inside an application
transaction that the step outcome store joins, the application commits and
these checks do not apply; the sink's commit of such a transaction is checked.

The check covers the Postgres sink and the Postgres step outcome store only,
whose writes gate the Kafka offset commit. The Key-Value Store and the replay
audit write in autocommit mode and are not covered; neither gates a Kafka
offset commit.

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

Errors these timeouts and checks give, in the adapter's ERROR lines for a
record that is not committed:

- A statement cancelled because of a statement or lock timeout, or "due to
  user request" (the JDBC query timeout, used inside an application
  transaction), or a socket read timeout: a Message Store statement took longer
  than `basis.message-store.statement-timeout` or waited longer than
  `basis.message-store.lock-timeout`. Check the database's health, its locks
  (`pg_locks`, `pg_stat_activity`) and its network before raising either value.
- A Message Store transaction "committed locally, but a synchronous standby did
  not confirm it": a synchronous commit's wait was cancelled. The record is read
  again and its outcome written again; check the synchronous standby and the
  database's replication status (`pg_stat_replication`).
- A Message Store transaction "committed, but the commit reported a warning":
  the commit succeeded but PostgreSQL reported a warning with it, which the
  adapter cannot tell apart from an unconfirmed commit. The record is read
  again, and fails again, until the warning's source is removed: for example a
  deferred constraint trigger with `RAISE WARNING` on a `bip_*` table. The error
  names the warning.

## Optional Kafka activity metrics

| Property | Environment mapping | Values | Default | Ownership |
| --- | --- | --- | --- | --- |
| `basis.traffic.signals` | `BASIS_TRAFFIC_SIGNALS` | Comma-separated `record_admitted`, `record_published`; empty disables | empty | Explicit integration event selection; opt-in |
| `basis.traffic.input-topics` | `BASIS_TRAFFIC_INPUT_TOPICS` | Exact comma-separated Kafka producer destination input topics; no wildcards | empty | Required for publication export; does not change routes or Kafka settings |

[Activity metrics](activity-metrics.md) defines admission and broker-ACK semantics,
static-producer requirements, fixed metrics and unknown coverage. Empty selection
preserves existing behavior. Export alone does not enable an alert policy.
