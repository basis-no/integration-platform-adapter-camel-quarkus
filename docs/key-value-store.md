# Key-Value Store

The Key-Value Store holds small JSON values for one
integration in its environment's Message Store: polling cursors, markers such
as "a full re-sync already ran", keys of input already seen, and small buffers.
It is not a place for payloads, secrets, or business records.

**Local 0.3.6 candidate; not published or certified.** This implements section 13 of the draft message-store
contract `v1.2.0-rc2`; it is not certified. It needs a Message Store provisioned
with `bip_key_value`. Against an older Message Store the adapter starts
and runs as before, and only Key-Value Store calls fail.

## Operations

Inject `KeyValueStore`. Keys belong to the integration named by `app.name`,
the identity recorded in `bip_message_log.integration`; without `app.name` every
state call fails. Values are Jackson `JsonNode` documents.

| Operation | Result |
| --- | --- |
| `get(key)` | value, version, expiry, and last update; or empty |
| `put(key, value[, ttl])` | the new version |
| `compareAndSet(key, expectedVersion, value[, ttl])` | the new version, or empty on a conflict that changed nothing |
| `putIfAbsent(key, value[, ttl])` | whether the key was created |
| `delete(key[, expectedVersion])` | whether a key was removed |
| `list(prefix[, after], limit)` | up to `limit` (1-1000) live keys with versions, in byte order, without values |

The version belongs to a physical row. It starts at 1 and increases on writes
while the row remains. An expired row is invisible; rewriting it before the
sweep continues that row's version. Once the sweep or an explicit delete removes
it, recreation starts at 1 again. A version read before deletion may therefore
match the new row, allowing a stale compare-and-set to succeed. These numbers
are not lease or fencing tokens. This candidate behavior is recorded explicitly
while its draft contract/version clarification awaits human review.

A `ttl` sets expiry from database time; `put` and `compareAndSet` without one
clear any expiry. Keep durable cursor keys without a TTL and do not delete them
as part of an ordinary advance. Existing polling locks remain necessary.

## Advancing a Cursor

Each call is its own single-row transaction, never part of a step outcome or
the message's status. A message that fails after a state write and is
redelivered or resent writes it again, so make writes safe to repeat. Advance a
cursor with `compareAndSet`; a conflict means another replica moved it first, so
re-read on the next poll:

```java
@ApplicationScoped
@Named("ledgerPoller")
public class LedgerPoller {
    private static final String CURSOR = "cursor/ledger";

    @Inject
    KeyValueStore state;

    public void poll() {
        Optional<KeyValueEntry> current = state.get(CURSOR);
        String after = current.map(entry -> entry.value().path("after").asText()).orElse("");
        String last = fetchAndPublishAfter(after);
        JsonNode next = JsonNodeFactory.instance.objectNode().put("after", last);
        if (current.isEmpty()) {
            state.putIfAbsent(CURSOR, next);
        } else {
            // Empty on conflict: another replica moved the cursor; re-read on the next poll.
            state.compareAndSet(CURSOR, current.get().version(), next);
        }
    }
}
```

Compare-and-set protects the stored cursor; it does not lock the preceding
fetch or publish. Several pollers can perform those effects before one wins the
write. Retain a qualified polling lock or use a tested stop-before-start process,
and make publication safe to repeat with a stable source message identity.
A crash after publish and before the cursor update may repeat publication. This
candidate adds no leases or transaction across state and message outcomes.

## Deduplicating Input

`idempotentRepository(prefix, ttl)` returns a Camel `IdempotentRepository` that
stores each id it sees as the key `<prefix>/<id>` with the required `ttl`:

```java
@Produces
@Named("seenRecords")
@ApplicationScoped
IdempotentRepository seenRecords(KeyValueStore state) {
    return state.idempotentRepository("seen/records", Duration.ofDays(7));
}
```

```yaml
- idempotentConsumer:
    idempotentRepository: seenRecords
    header: recordId
    steps:
      - to: "direct:publish-record"
```

It is sized for source-level deduplication, such as a poll that returns
overlapping records: every id seen within the `ttl` is a live key and counts
toward the integration's limit. Key it by the source's identity. Do not key it
by `x-bip-message-id` on a Kafka boundary: a resend reuses the message id and
would be dropped.

## Limits and Errors

- Keys are 1-256 characters without control characters; `/` groups keys by
  convention only.
- Values are at most 64 KiB of JSON text.
- The insertion threshold is 10 000 live keys. A new key observing that count
  fails; updates of existing keys pass. Concurrent inserts can overshoot the
  threshold, so it is not a strict serialized quota.

A rejected write throws `KeyValueLimitException`; repeating it fails the
same way. An unavailable store throws `KeyValueStoreException`, as does a
Message Store without `bip_key_value`, with a message that the Key-Value Store needs Message Store contract 1.2.

## Expiry and Environments

Once state is used, the adapter deletes this integration's expired keys in
batches of 500, immediately and then every
`basis.message-store.key-value.sweep-interval-seconds` (default and maximum 3600).
Without the table the sweep stays idle. Message retention and payload deletion
never delete state.

Each environment has its own Message Store and so its own state. Promotion
copies no state; a new environment starts empty.

See [Configuration](configuration.md) for the exact property.

## Application database continuity

The Key-Value Store is optional. An integration can keep using its existing
application database and schema without migrating cursors or changing business
logic when it upgrades the adapter. Keep that database on a named datasource,
such as `application`, separate from the default Message Store datasource.

Moving cursors later is a separate migration: preserve stable business message
identities, the existing polling fence and atomic reset. Use no TTL for durable
cursors. After new writes, reconcile the latest state before rolling code back.

## Provisioning and isolation

The platform owns the contract migrations and runtime database grants. This API
uses the same default JDBC Message Store datasource and integration identity as
the maintenance adapter. It never creates tables, renames the old table, or
changes contract metadata. It does not fall back to `bip_integration_state`.
A deployment with a different application datasource must supply the correctly
scoped store bean rather than point this API at a business database. Where
per-integration database isolation is enabled, qualify the API with the actual
runtime and platform reader logins and the platform's policies. An owner-role
fixture proves API wiring, but cannot prove those permission boundaries.

Rows belong to one environment and do not move with image promotion. Rollback
after new state writes must reconcile the latest state before an older
application resumes; changing the adapter pin alone does not move data back.

## Candidate qualification fixtures

The ordinary maintenance smoke includes a lazy-CDI check with its datasource
inactive. A separate `integration-tests/key-value-smoke` fixture boots Quarkus
against one isolated PostgreSQL 17 container pinned to the platform test image and exercises the injected API,
integration identity, contract metadata preservation and application disposal.
Its synthetic database binds to loopback and never accepts a supplied JDBC URL.
The pinned image must already be cached; this fixture refuses automatic image
pulls. It requires a fresh `bip.test.run-id`, supplied to Maven and its test fork,
for exact container ownership. Memory and swap are both bounded to 256 MiB,
with one CPU and 64 PIDs. Partial startup and schema failures stop the owned
database and preserve the first failure. Image preparation needs its own
resource window.
These fixtures exercise candidate API behavior and runtime wiring. They do not
certify the draft contract or least-privileged database permissions. With Docker
available and the pinned image cached, install the candidate locally, then run
`./mvnw -f integration-tests/maintenance-smoke/pom.xml verify` and
the PostgreSQL fixture with an explicit unique run ID, for example:

```sh
run_id="$(uuidgen)"
./mvnw -f integration-tests/key-value-smoke/pom.xml \
  -Dbip.test.run-id="$run_id" \
  -DargLine="-Dbip.test.run-id=$run_id" verify
```

The root `PostgresKeyValueStoreTest` supports the maintenance suite's opt-in
`-Dbip.test.bounded-services=true` mode. Pass that flag and the same unique
`bip.test.run-id` to Maven and its test fork. This mode uses the frozen cached
PostgreSQL 16 digest, refuses image pulls, bounds memory and swap to 256 MiB,
CPU to one and PIDs to 64, and publishes all ports on loopback. Ordinary CI
keeps its existing container setup when the flag is absent.
The S33–S38 protocol tests and least-privileged RLS proof remain separate gates.

SQL execution is bounded to five seconds per statement; connection acquisition
and network timeouts remain the datasource's responsibility. A cancelled or
failed write may need a read-back before retrying: the caller must not assume
that every transport error means the write did not commit. Shutdown interrupts
the bounded-batch sweeper and waits up to ten seconds, reporting a failure if
it cannot prove termination.
