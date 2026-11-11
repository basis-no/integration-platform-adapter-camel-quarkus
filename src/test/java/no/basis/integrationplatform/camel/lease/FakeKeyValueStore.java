package no.basis.integrationplatform.camel.lease;

import com.fasterxml.jackson.databind.JsonNode;
import no.basis.integrationplatform.camel.keyvalue.KeyValueStore;
import no.basis.integrationplatform.camel.keyvalue.KeyValueEntry;
import no.basis.integrationplatform.camel.keyvalue.KeyValueStoreException;
import no.basis.integrationplatform.camel.keyvalue.KeyValueKey;
import no.basis.integrationplatform.camel.keyvalue.KeyValueLimitException;
import org.apache.camel.spi.IdempotentRepository;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * In-memory Key-Value Store with the Postgres store's semantics for the operations
 * the lease protocol uses, on a test clock, with switchable failure modes and
 * hooks that run inside a call so a test can interleave events deterministically.
 */
final class FakeKeyValueStore implements KeyValueStore {
    record Row(JsonNode value, long version, Instant expiresAt) {
    }

    final Map<String, Row> rows = new LinkedHashMap<>();
    final java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
    final java.util.concurrent.atomic.AtomicInteger versionedDeletes = new java.util.concurrent.atomic.AtomicInteger();
    private final TestClock clock;
    volatile boolean provisioned = true;
    volatile boolean reachable = true;
    volatile boolean atKeyLimit;
    volatile Runnable beforeGet = () -> {
    };
    volatile Runnable afterPutIfAbsent = () -> {
    };
    volatile Runnable beforePutIfAbsent = () -> {
    };
    volatile Runnable beforeDelete = () -> {
    };
    volatile Runnable beforeCompareAndSet = () -> {
    };
    volatile Runnable afterCompareAndSet = () -> {
    };

    FakeKeyValueStore(TestClock clock) {
        this.clock = clock;
    }

    synchronized Row live(String key) {
        Row row = rows.get(key);
        return row == null || (row.expiresAt() != null && !row.expiresAt().isAfter(clock.instant())) ? null : row;
    }

    synchronized void hold(String key, String holder, Duration ttl) {
        put(key, com.fasterxml.jackson.databind.node.JsonNodeFactory.instance.objectNode().put("holder", holder), ttl);
    }

    /** Blocks until the latch opens, ignoring interrupts as a JDBC driver blocked in a socket read does. */
    static void blockOn(java.util.concurrent.CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public Optional<KeyValueEntry> get(String key) {
        calls.incrementAndGet();
        beforeGet.run();
        synchronized (this) {
            check();
            Row row = live(key);
            return row == null
                    ? Optional.empty()
                    : Optional.of(new KeyValueEntry(row.value(), row.version(), row.expiresAt(),
                            clock.instant(), "runtime"));
        }
    }

    @Override
    public synchronized long put(String key, JsonNode value, Duration ttl) {
        check();
        Row existing = rows.get(key);
        long version = existing == null ? 1 : existing.version() + 1;
        if (existing == null && atKeyLimit) {
            throw limit();
        }
        rows.put(key, new Row(value, version, expiresAt(ttl)));
        return version;
    }

    @Override
    public OptionalLong compareAndSet(String key, long expectedVersion, JsonNode value, Duration ttl) {
        calls.incrementAndGet();
        beforeCompareAndSet.run();
        synchronized (this) {
            check();
            Row row = live(key);
            if (row == null || row.version() != expectedVersion) {
                return OptionalLong.empty();
            }
            rows.put(key, new Row(value, row.version() + 1, expiresAt(ttl)));
            afterCompareAndSet.run();
            return OptionalLong.of(row.version() + 1);
        }
    }

    @Override
    public boolean putIfAbsent(String key, JsonNode value, Duration ttl) {
        calls.incrementAndGet();
        beforePutIfAbsent.run();
        boolean created;
        synchronized (this) {
            check();
            if (live(key) != null) {
                created = false;
            } else {
                Row expired = rows.get(key);
                if (expired == null && atKeyLimit) {
                    throw limit();
                }
                rows.put(key, new Row(value, expired == null ? 1 : expired.version() + 1, expiresAt(ttl)));
                created = true;
            }
        }
        afterPutIfAbsent.run();
        return created;
    }

    @Override
    public synchronized boolean delete(String key) {
        check();
        return live(key) != null && rows.remove(key) != null;
    }

    @Override
    public boolean delete(String key, long expectedVersion) {
        beforeDelete.run();
        synchronized (this) {
            check();
            Row row = live(key);
            boolean removed = row != null && row.version() == expectedVersion && rows.remove(key) != null;
            if (removed) {
                versionedDeletes.incrementAndGet();
            }
            return removed;
        }
    }

    @Override
    public List<KeyValueKey> list(String prefix, String after, int limit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IdempotentRepository idempotentRepository(String prefix, Duration ttl) {
        throw new UnsupportedOperationException();
    }

    private void check() {
        if (!reachable) {
            throw unreachable();
        }
        if (!provisioned) {
            // As the Postgres store reports it: the store's failure with the database's undefined-table state.
            throw new KeyValueStoreException("Key-Value Store needs a provisioned bip_key_value table",
                    new SQLException("relation \"bip_key_value\" does not exist", "42P01"));
        }
    }

    static KeyValueStoreException unreachable() {
        return new KeyValueStoreException("Key-Value Store failed: connection refused",
                new SQLException("connection refused", "08001"));
    }

    private static KeyValueLimitException limit() {
        return new KeyValueLimitException("key limit reached", new SQLException("54000"));
    }

    private Instant expiresAt(Duration ttl) {
        return ttl == null ? null : clock.instant().plus(ttl);
    }
}
