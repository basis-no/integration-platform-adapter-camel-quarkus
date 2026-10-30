package no.basis.integrationplatform.camel.keyvalue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.spi.IdempotentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Key-Value Store in the Message Store table {@code bip_key_value} (contract section 13). */
final class PostgresKeyValueStore implements KeyValueStore, AutoCloseable {
    static final String TABLE_NAME = "bip_key_value";
    static final int MAX_KEY_CHARACTERS = 256;
    static final int MAX_VALUE_BYTES = 65_536;
    static final int MAX_LIST_LIMIT = 1_000;
    static final int DELETE_BATCH_SIZE = 500;
    static final int STATEMENT_TIMEOUT_SECONDS = 5;

    private static final Logger LOG = LoggerFactory.getLogger(PostgresKeyValueStore.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String UNDEFINED_TABLE = "42P01";
    private static final String KEY_LIMIT = "54000";
    private static final String CHECK_VIOLATION = "23514";
    private static final String DATA_EXCEPTION_CLASS = "22";

    private static final String GET_SQL = """
            select value::text, version, expires_at, updated_at, updated_by
            from bip_key_value
            where integration = ?
              and key = ?
              and (expires_at is null or expires_at > current_timestamp)
            """;

    // A null ttl makes expires_at null, which clears any earlier expiry.
    private static final String UPSERT_SQL = """
            insert into bip_key_value (integration, key, value, expires_at, updated_by)
            values (?, ?, ?::jsonb, current_timestamp + ?::bigint * interval '1 millisecond', 'runtime')
            on conflict (integration, key) do update
            set value = excluded.value,
                version = bip_key_value.version + 1,
                updated_at = current_timestamp,
                expires_at = excluded.expires_at,
                updated_by = 'runtime'
            """;

    private static final String PUT_SQL = UPSERT_SQL + "returning version";

    // Only an expired row may be overwritten; it starts a new life and continues its version.
    private static final String PUT_IF_ABSENT_SQL = UPSERT_SQL + """
            where bip_key_value.expires_at is not null
              and bip_key_value.expires_at <= current_timestamp
            returning version
            """;

    private static final String COMPARE_AND_SET_SQL = """
            update bip_key_value
            set value = ?::jsonb,
                version = version + 1,
                updated_at = current_timestamp,
                expires_at = current_timestamp + ?::bigint * interval '1 millisecond',
                updated_by = 'runtime'
            where integration = ?
              and key = ?
              and version = ?
              and (expires_at is null or expires_at > current_timestamp)
            returning version
            """;

    private static final String DELETE_SQL = """
            delete from bip_key_value
            where integration = ?
              and key = ?
              and (expires_at is null or expires_at > current_timestamp)
            """;

    private static final String DELETE_AT_VERSION_SQL = """
            delete from bip_key_value
            where integration = ?
              and key = ?
              and version = ?
              and (expires_at is null or expires_at > current_timestamp)
            """;

    // left() rather than LIKE so a prefix needs no escaping; "C" collation orders by bytes.
    private static final String LIST_SQL = """
            select key, version, expires_at, updated_at, updated_by
            from bip_key_value
            where integration = ?
              and left(key, length(?::text)) = ?
              and key collate "C" > ?
              and (expires_at is null or expires_at > current_timestamp)
            order by key collate "C"
            limit ?
            """;

    private static final String DELETE_PREFIX_BATCH_SQL = """
            delete from bip_key_value
            where integration = ?
              and key in (
                  select key
                  from bip_key_value
                  where integration = ?
                    and left(key, length(?::text)) = ?
                  limit ?)
            """;

    // The outer expiry condition is rechecked on a row a concurrent write revived,
    // so the sweep never deletes a live key.
    private static final String SWEEP_BATCH_SQL = """
            delete from bip_key_value
            where integration = ?
              and expires_at <= current_timestamp
              and key in (
                  select key
                  from bip_key_value
                  where integration = ?
                    and expires_at <= current_timestamp
                  limit ?)
            """;

    // Resolved through the search path, like every other statement here.
    private static final String TABLE_EXISTS_SQL = "select to_regclass('" + TABLE_NAME + "') is not null";

    private final DataSource dataSource;
    private final String integration;
    private ScheduledExecutorService sweeper;

    PostgresKeyValueStore(DataSource dataSource, String integration) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.integration = Objects.requireNonNull(integration, "integration");
    }

    @Override
    public Optional<KeyValueEntry> get(String key) {
        validateKey(key);
        return query("read key " + key, GET_SQL, statement -> {
            statement.setString(1, integration);
            statement.setString(2, key);
        }, resultSet -> resultSet.next()
                ? Optional.of(new KeyValueEntry(
                        readJson(resultSet.getString(1)),
                        resultSet.getLong(2),
                        instant(resultSet.getTimestamp(3)),
                        instant(resultSet.getTimestamp(4)),
                        resultSet.getString(5)))
                : Optional.empty());
    }

    @Override
    public long put(String key, JsonNode value, Duration ttl) {
        return upsert(PUT_SQL, key, value, ttl).orElseThrow();
    }

    @Override
    public OptionalLong compareAndSet(String key, long expectedVersion, JsonNode value, Duration ttl) {
        validateKey(key);
        String json = writeJson(value);
        Long ttlMillis = ttlMillis(ttl);
        return query("write key " + key, COMPARE_AND_SET_SQL, statement -> {
            statement.setString(1, json);
            statement.setObject(2, ttlMillis, Types.BIGINT);
            statement.setString(3, integration);
            statement.setString(4, key);
            statement.setLong(5, expectedVersion);
        }, PostgresKeyValueStore::version);
    }

    @Override
    public boolean putIfAbsent(String key, JsonNode value, Duration ttl) {
        return upsert(PUT_IF_ABSENT_SQL, key, value, ttl).isPresent();
    }

    @Override
    public boolean delete(String key) {
        validateKey(key);
        return update("delete key " + key, DELETE_SQL, statement -> {
            statement.setString(1, integration);
            statement.setString(2, key);
        }) > 0;
    }

    @Override
    public boolean delete(String key, long expectedVersion) {
        validateKey(key);
        return update("delete key " + key, DELETE_AT_VERSION_SQL, statement -> {
            statement.setString(1, integration);
            statement.setString(2, key);
            statement.setLong(3, expectedVersion);
        }) > 0;
    }

    @Override
    public List<KeyValueKey> list(String prefix, String after, int limit) {
        Objects.requireNonNull(prefix, "prefix");
        if (limit < 1 || limit > MAX_LIST_LIMIT) {
            throw new IllegalArgumentException("Key-Value Store list limit must be 1 to " + MAX_LIST_LIMIT);
        }
        return query("list prefix " + prefix, LIST_SQL, statement -> {
            statement.setString(1, integration);
            statement.setString(2, prefix);
            statement.setString(3, prefix);
            // Every key is longer than the empty string, so "" starts at the first key.
            statement.setString(4, after == null ? "" : after);
            statement.setInt(5, limit);
        }, resultSet -> {
            List<KeyValueKey> keys = new ArrayList<>();
            while (resultSet.next()) {
                keys.add(new KeyValueKey(
                        resultSet.getString(1),
                        resultSet.getLong(2),
                        instant(resultSet.getTimestamp(3)),
                        instant(resultSet.getTimestamp(4)),
                        resultSet.getString(5)));
            }
            return keys;
        });
    }

    @Override
    public IdempotentRepository idempotentRepository(String prefix, Duration ttl) {
        validateKey(prefix);
        if (ttl == null || ttl.isNegative() || ttl.isZero()) {
            throw new IllegalArgumentException("A key-value store idempotent repository needs a positive ttl");
        }
        return new KeyValueIdempotentRepository(this, prefix + "/", ttl);
    }

    /** Deletes every key with the prefix, live or expired, in bounded batches. */
    int deletePrefix(String prefix) {
        return deleteInBatches("delete prefix " + prefix, DELETE_PREFIX_BATCH_SQL, statement -> {
            statement.setString(1, integration);
            statement.setString(2, integration);
            statement.setString(3, prefix);
            statement.setString(4, prefix);
            statement.setInt(5, DELETE_BATCH_SIZE);
        });
    }

    /**
     * Deletes this integration's expired keys in bounded batches. A Message Store
     * before contract 1.2 has no state table; the sweep then stays idle.
     */
    int sweepExpired() {
        boolean provisioned = query("check for " + TABLE_NAME, TABLE_EXISTS_SQL, statement -> {
        }, resultSet -> resultSet.next() && resultSet.getBoolean(1));
        if (!provisioned) {
            LOG.debug("Key-Value Store sweep idle: the Message Store has no {} table", TABLE_NAME);
            return 0;
        }
        int deleted = deleteInBatches("delete expired keys", SWEEP_BATCH_SQL, statement -> {
            statement.setString(1, integration);
            statement.setString(2, integration);
            statement.setInt(3, DELETE_BATCH_SIZE);
        });
        LOG.debug("Key-Value Store sweep deleted {} expired keys", deleted);
        return deleted;
    }

    synchronized void startSweep(Duration interval) {
        if (sweeper != null) {
            return;
        }
        sweeper = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "bip-key-value-sweep");
            thread.setDaemon(true);
            return thread;
        });
        sweeper.scheduleWithFixedDelay(this::sweepQuietly, 0, interval.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized void close() {
        if (sweeper != null) {
            sweeper.shutdownNow();
            try {
                if (!sweeper.awaitTermination(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Key-Value Store sweeper did not stop within its cleanup bound");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while stopping Key-Value Store sweeper", interrupted);
            }
            sweeper = null;
        }
    }

    private void sweepQuietly() {
        try {
            sweepExpired();
        } catch (RuntimeException e) {
            // An exception would cancel the schedule; the next run retries.
            LOG.warn("Key-Value Store sweep failed; retrying at the next interval", e);
        }
    }

    private OptionalLong upsert(String sql, String key, JsonNode value, Duration ttl) {
        validateKey(key);
        String json = writeJson(value);
        Long ttlMillis = ttlMillis(ttl);
        return query("write key " + key, sql, statement -> {
            statement.setString(1, integration);
            statement.setString(2, key);
            statement.setString(3, json);
            statement.setObject(4, ttlMillis, Types.BIGINT);
        }, PostgresKeyValueStore::version);
    }

    private int deleteInBatches(String operation, String sql, Binder binder) {
        int deleted = 0;
        int batch;
        do {
            if (Thread.currentThread().isInterrupted()) {
                throw new KeyValueStoreException("Interrupted while attempting to " + operation,
                        new InterruptedException("Key-Value Store batch interrupted"));
            }
            batch = update(operation, sql, binder);
            deleted += batch;
        } while (batch == DELETE_BATCH_SIZE);
        return deleted;
    }

    private <T> T query(String operation, String sql, Binder binder, Reader<T> reader) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setQueryTimeout(STATEMENT_TIMEOUT_SECONDS);
            binder.bind(statement);
            try (ResultSet resultSet = statement.executeQuery()) {
                return reader.read(resultSet);
            }
        } catch (SQLException e) {
            throw failure(operation, e);
        }
    }

    private int update(String operation, String sql, Binder binder) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setQueryTimeout(STATEMENT_TIMEOUT_SECONDS);
            binder.bind(statement);
            return statement.executeUpdate();
        } catch (SQLException e) {
            throw failure(operation, e);
        }
    }

    private static RuntimeException failure(String operation, SQLException e) {
        String sqlState = e.getSQLState();
        if (UNDEFINED_TABLE.equals(sqlState)) {
            return new KeyValueStoreException("Key-Value Store needs Message Store contract 1.2: this"
                    + " Message Store has no " + TABLE_NAME + " table (" + operation + ")", e);
        }
        // Constraint and data errors repeat on every retry; they are not an unavailable store.
        if (KEY_LIMIT.equals(sqlState) || CHECK_VIOLATION.equals(sqlState)
                || (sqlState != null && sqlState.startsWith(DATA_EXCEPTION_CLASS))) {
            return new KeyValueLimitException(
                    "Key-Value Store rejected the write (" + operation + "): " + e.getMessage(), e);
        }
        return new KeyValueStoreException("Key-Value Store failed to " + operation, e);
    }

    private static OptionalLong version(ResultSet resultSet) throws SQLException {
        return resultSet.next() ? OptionalLong.of(resultSet.getLong(1)) : OptionalLong.empty();
    }

    static void validateKey(String key) {
        if (key == null || key.isEmpty() || key.codePointCount(0, key.length()) > MAX_KEY_CHARACTERS
                || key.codePoints().anyMatch(Character::isISOControl)) {
            throw new KeyValueLimitException(
                    "Key-Value Store keys are 1 to " + MAX_KEY_CHARACTERS + " characters without control characters");
        }
    }

    private static String writeJson(JsonNode value) {
        Objects.requireNonNull(value, "value");
        byte[] json;
        try {
            json = JSON.writeValueAsBytes(value);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Key-Value Store value is not a JSON document", e);
        }
        if (json.length > MAX_VALUE_BYTES) {
            throw new KeyValueLimitException("Key-Value Store values are at most " + MAX_VALUE_BYTES
                    + " bytes of JSON; this value has " + json.length);
        }
        return new String(json, StandardCharsets.UTF_8);
    }

    private static JsonNode readJson(String json) {
        try {
            return JSON.readTree(json);
        } catch (JsonProcessingException e) {
            throw new KeyValueStoreException("Key-Value Store value is not valid JSON", e);
        }
    }

    private static Long ttlMillis(Duration ttl) {
        if (ttl == null) {
            return null;
        }
        if (ttl.isNegative() || ttl.isZero()) {
            throw new IllegalArgumentException("Key-Value Store ttl must be positive");
        }
        return ttl.toMillis();
    }

    private static Instant instant(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toInstant();
    }

    @FunctionalInterface
    private interface Binder {
        void bind(PreparedStatement statement) throws SQLException;
    }

    @FunctionalInterface
    private interface Reader<T> {
        T read(ResultSet resultSet) throws SQLException;
    }
}
