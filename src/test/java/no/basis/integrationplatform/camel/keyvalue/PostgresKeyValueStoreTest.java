package no.basis.integrationplatform.camel.keyvalue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.TextNode;
import no.basis.integrationplatform.camel.TestJdbc;
import org.apache.camel.CamelExecutionException;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.spi.IdempotentRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class PostgresKeyValueStoreTest {
    @Container
    private static final PostgreSQLContainer POSTGRES = boundedPostgres();

    private static PostgreSQLContainer boundedPostgres() {
        boolean bounded = Boolean.getBoolean("bip.test.bounded-services");
        String runId = System.getProperty("bip.test.run-id");
        if (bounded && (runId == null || runId.isBlank())) {
            throw new IllegalArgumentException("Unique bip.test.run-id is required");
        }
        var postgres = new PostgreSQLContainer(
                bounded ? "postgres@sha256:16bc17c64a573ef34162af9298258d1aec548232985b33ed7b1eac33ba35c229" : "postgres:16-alpine");
        if (bounded) {
            postgres.withImagePullPolicy(image -> false);
            postgres.withLabel("bip.qualification.run", runId);
            postgres.withCreateContainerCmdModifier(command -> {
                var ports = command.getExposedPorts();
                if (ports == null || ports.length == 0) throw new IllegalStateException("Missing synthetic fixture ports");
                var bindings = java.util.Arrays.stream(ports)
                        .map(port -> new com.github.dockerjava.api.model.PortBinding(
                                com.github.dockerjava.api.model.Ports.Binding.bindIp("127.0.0.1"), port))
                        .toArray(com.github.dockerjava.api.model.PortBinding[]::new);
                command.getHostConfig().withMemory(256L * 1024 * 1024).withMemorySwap(256L * 1024 * 1024)
                        .withPidsLimit(64L).withNanoCPUs(1_000_000_000L).withPortBindings(bindings);
            });
        }
        return postgres;
    }

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String INTEGRATION = "orders-sync";
    private static final String OTHER_INTEGRATION = "invoice-sync";

    private DataSource dataSource;
    private PostgresKeyValueStore state;
    private PostgresKeyValueStore otherState;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = dataSource();
        TestJdbc.awaitReachable(dataSource);
        execute("drop table if exists " + PostgresKeyValueStore.TABLE_NAME);
        execute("drop table if exists bip_contract");
        execute(schemaSql("bip_contract.sql"));
        execute(schemaSql("bip_key_value.sql"));
        state = new PostgresKeyValueStore(dataSource, INTEGRATION);
        otherState = new PostgresKeyValueStore(dataSource, OTHER_INTEGRATION);
    }

    @AfterEach
    void tearDown() {
        RuntimeException failure = null;
        try { if (state != null) state.close(); }
        catch (RuntimeException error) { failure = error; }
        try { if (otherState != null) otherState.close(); }
        catch (RuntimeException error) {
            if (failure == null) failure = error; else failure.addSuppressed(error);
        }
        if (failure != null) throw failure;
    }

    @Test
    void blockedDatabaseCallTimesOutAsStoreUnavailable() throws Exception {
        try (Connection lock = dataSource.getConnection(); Statement statement = lock.createStatement()) {
            lock.setAutoCommit(false);
            statement.execute("lock table bip_key_value in access exclusive mode");
            org.junit.jupiter.api.Assertions.assertTimeoutPreemptively(Duration.ofSeconds(8), () -> {
                KeyValueStoreException failure = assertThrows(KeyValueStoreException.class,
                        () -> state.get("cursor/blocked"));
                assertEquals("57014", ((SQLException) failure.getCause()).getSQLState());
            });
        }
    }

    @Test
    void putVersionsEveryWriteAndGetReadsTheLatest() throws Exception {
        assertEquals(1, state.put("cursor/ledger", json("{\"after\":1}")));
        assertEquals(2, state.put("cursor/ledger", json("{\"after\":2}")));

        KeyValueEntry entry = state.get("cursor/ledger").orElseThrow();
        assertEquals(json("{\"after\":2}"), entry.value());
        assertEquals(2, entry.version());
        assertEquals("runtime", entry.updatedBy());
        assertNull(entry.expiresAt());
        assertNotNull(entry.updatedAt());
        assertEquals(1, count("select count(*) from bip_key_value"));
        assertTrue(state.get("cursor/missing").isEmpty());

        state.put("cursor/ledger", json("{\"after\":3}"), Duration.ofHours(1));
        assertEquals(3600, count("select extract(epoch from expires_at - updated_at)::bigint from bip_key_value"),
                "the ttl counts from the database's time of the write");
        state.put("cursor/ledger", json("{\"after\":4}"));
        assertNull(state.get("cursor/ledger").orElseThrow().expiresAt(), "a put without ttl clears the expiry");
    }

    @Test
    void compareAndSetLetsExactlyOneConcurrentWriterWin() throws Exception {
        state.put("cursor/ledger", TextNode.valueOf("start"));
        CyclicBarrier start = new CyclicBarrier(2);
        ExecutorService writers = Executors.newFixedThreadPool(2);
        try {
            Future<OptionalLong> first = writers.submit(() -> {
                start.await();
                return state.compareAndSet("cursor/ledger", 1, TextNode.valueOf("first"));
            });
            Future<OptionalLong> second = writers.submit(() -> {
                start.await();
                return state.compareAndSet("cursor/ledger", 1, TextNode.valueOf("second"));
            });
            OptionalLong firstResult = first.get(30, TimeUnit.SECONDS);
            OptionalLong secondResult = second.get(30, TimeUnit.SECONDS);

            assertTrue(firstResult.isPresent() ^ secondResult.isPresent(), "exactly one writer wins");
            OptionalLong winner = firstResult.isPresent() ? firstResult : secondResult;
            assertEquals(2, winner.getAsLong());
            KeyValueEntry entry = state.get("cursor/ledger").orElseThrow();
            assertEquals(TextNode.valueOf(firstResult.isPresent() ? "first" : "second"), entry.value());
            assertEquals(2, entry.version());
        } finally {
            writers.shutdownNow();
        }

        assertTrue(state.compareAndSet("cursor/ledger", 1, TextNode.valueOf("stale")).isEmpty());
        assertEquals(2, state.get("cursor/ledger").orElseThrow().version());
        assertTrue(state.compareAndSet("cursor/missing", 1, TextNode.valueOf("new")).isEmpty(),
                "compare-and-set never creates a key");
        assertTrue(state.get("cursor/missing").isEmpty());
    }

    @Test
    void putIfAbsentCreatesOnceAndAgainAfterExpiryWithTheNextVersion() throws Exception {
        assertTrue(state.putIfAbsent("marker/full-resync", BooleanNode.TRUE, Duration.ofMinutes(5)));
        assertFalse(state.putIfAbsent("marker/full-resync", BooleanNode.FALSE, Duration.ofMinutes(5)));
        assertEquals(BooleanNode.TRUE, state.get("marker/full-resync").orElseThrow().value());

        expire(INTEGRATION, "marker/full-resync");

        assertTrue(state.get("marker/full-resync").isEmpty());
        assertTrue(state.list("marker/", 10).isEmpty());
        assertTrue(state.compareAndSet("marker/full-resync", 1, BooleanNode.FALSE).isEmpty());
        assertFalse(state.delete("marker/full-resync"));
        assertTrue(state.putIfAbsent("marker/full-resync", BooleanNode.FALSE, Duration.ofMinutes(5)));
        KeyValueEntry entry = state.get("marker/full-resync").orElseThrow();
        assertEquals(BooleanNode.FALSE, entry.value());
        assertEquals(2, entry.version(), "an unswept expired row continues its version");
        assertTrue(state.compareAndSet("marker/full-resync", 1, BooleanNode.TRUE).isEmpty(),
                "The previous row version must conflict before physical cleanup");
    }

    @Test
    void recreatingASweptExpiredKeyStartsANewRowAtVersionOne() throws Exception {
        assertEquals(1, state.put("marker/swept", BooleanNode.TRUE, Duration.ofMinutes(5)));
        assertEquals(2, state.put("marker/swept", BooleanNode.FALSE, Duration.ofMinutes(5)));
        expire(INTEGRATION, "marker/swept");
        assertEquals(1, state.sweepExpired());
        assertTrue(state.get("marker/swept").isEmpty());
        assertEquals(1, state.put("marker/swept", BooleanNode.TRUE));
    }

    @Test
    void recreatingAnExplicitlyDeletedKeyStartsANewRowAtVersionOne() {
        assertEquals(1, state.put("marker/deleted", BooleanNode.TRUE));
        assertEquals(2, state.put("marker/deleted", BooleanNode.FALSE));
        assertTrue(state.delete("marker/deleted", 2));
        assertEquals(1, state.put("marker/deleted", BooleanNode.TRUE));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void staleNumericVersionCanMatchARecreatedRow(boolean swept) throws Exception {
        long staleVersion = state.put("cursor/recreated", json("{\"owner\":\"old\"}"), Duration.ofMinutes(5));
        if (swept) {
            expire(INTEGRATION, "cursor/recreated");
            assertEquals(1, state.sweepExpired());
        } else {
            assertTrue(state.delete("cursor/recreated", staleVersion));
        }
        assertEquals(1, state.put("cursor/recreated", json("{\"owner\":\"new\"}")));
        // This records the candidate limitation, not an ownership guarantee.
        assertTrue(state.compareAndSet("cursor/recreated", staleVersion,
                json("{\"owner\":\"stale-writer\"}")).isPresent());
        assertEquals("stale-writer", state.get("cursor/recreated").orElseThrow().value().path("owner").asText());
    }

    @Test
    void deleteRemovesALiveKeyOnlyAtTheExpectedVersion() {
        state.put("cursor/ledger", BooleanNode.TRUE);
        state.put("cursor/ledger", BooleanNode.FALSE);

        assertFalse(state.delete("cursor/ledger", 1));
        assertFalse(otherState.delete("cursor/ledger"));
        assertTrue(state.delete("cursor/ledger", 2));
        assertFalse(state.delete("cursor/ledger"));
        state.put("cursor/ledger", BooleanNode.TRUE);
        assertTrue(state.delete("cursor/ledger"));
    }

    @Test
    void sweepDeletesOnlyThisIntegrationsExpiredKeysInBatches() throws Exception {
        int expired = 2 * PostgresKeyValueStore.DELETE_BATCH_SIZE + 1;
        insertKeys(INTEGRATION, "seen/", expired, "current_timestamp - interval '1 minute'");
        insertKeys(OTHER_INTEGRATION, "seen/", 3, "current_timestamp - interval '1 minute'");
        state.put("cursor/ledger", BooleanNode.TRUE, Duration.ofHours(1));
        state.put("marker/done", BooleanNode.TRUE);

        assertEquals(expired, state.sweepExpired());

        assertEquals(2, count("select count(*) from bip_key_value where integration = '" + INTEGRATION + "'"));
        assertEquals(3, count("select count(*) from bip_key_value where integration = '"
                + OTHER_INTEGRATION + "'"));
    }

    @Test
    void scheduledSweepDeletesExpiredKeysWithoutACall() throws Exception {
        insertKeys(INTEGRATION, "seen/", 3, "current_timestamp - interval '1 minute'");

        state.startSweep(Duration.ofMillis(100));

        Instant deadline = Instant.now().plusSeconds(30);
        while (count("select count(*) from bip_key_value") > 0 && Instant.now().isBefore(deadline)) {
            Thread.sleep(50);
        }
        assertEquals(0, count("select count(*) from bip_key_value"));
    }

    @Test
    void rejectsKeysAndValuesOutsideTheLimitsWithoutWriting() throws Exception {
        assertThrows(KeyValueLimitException.class, () -> state.put("k".repeat(257), BooleanNode.TRUE));
        assertThrows(KeyValueLimitException.class, () -> state.put("", BooleanNode.TRUE));
        assertThrows(KeyValueLimitException.class, () -> state.put("cursor/\nledger", BooleanNode.TRUE));
        assertThrows(KeyValueLimitException.class,
                () -> state.put("buffer/large", TextNode.valueOf("x".repeat(PostgresKeyValueStore.MAX_VALUE_BYTES))));
        assertEquals(1, state.put("k".repeat(256), BooleanNode.TRUE));

        // Compact JSON within the limit whose jsonb text form is not: the database stays authoritative.
        ArrayNode ones = JsonNodeFactory.instance.arrayNode();
        for (int i = 0; i < 30_000; i++) {
            ones.add(1);
        }
        KeyValueLimitException rejected = assertThrows(KeyValueLimitException.class,
                () -> state.put("buffer/ones", ones));
        assertEquals("23514", ((SQLException) rejected.getCause()).getSQLState());
        assertEquals(1, count("select count(*) from bip_key_value"));
    }

    @Test
    void rejectsTheKeyBeyondTheLiveKeyLimitWhileUpdatesPass() throws Exception {
        insertKeys(INTEGRATION, "seen/", 10_000, "null");

        KeyValueLimitException rejected = assertThrows(KeyValueLimitException.class,
                () -> state.put("seen/10001", BooleanNode.TRUE));
        assertEquals("54000", ((SQLException) rejected.getCause()).getSQLState());
        assertThrows(KeyValueLimitException.class, () -> state.putIfAbsent("seen/10001", BooleanNode.TRUE));

        assertEquals(2, state.put("seen/1", BooleanNode.FALSE));
        assertEquals(1, otherState.put("seen/1", BooleanNode.TRUE));
        assertEquals(10_000, count("select count(*) from bip_key_value where integration = '"
                + INTEGRATION + "'"));
    }

    @Test
    void listsOwnLiveKeysWithAPrefixInByteOrderAndPagesAfterAKey() throws Exception {
        for (String key : List.of("cursor/b", "cursor/a", "cursor/C", "cursorX", "50%/x", "500/x")) {
            state.put(key, BooleanNode.TRUE);
        }
        state.put("cursor/expired", BooleanNode.TRUE, Duration.ofMinutes(5));
        expire(INTEGRATION, "cursor/expired");
        otherState.put("cursor/a", BooleanNode.TRUE);
        otherState.put("cursor/z", BooleanNode.TRUE);

        List<KeyValueKey> firstPage = state.list("cursor/", 2);
        assertEquals(List.of("cursor/C", "cursor/a"), keys(firstPage));
        assertEquals(1, firstPage.get(0).version());
        assertEquals("runtime", firstPage.get(0).updatedBy());
        assertEquals(List.of("cursor/b"), keys(state.list("cursor/", "cursor/a", 2)));
        assertEquals(List.of("cursor/a", "cursor/z"), keys(otherState.list("cursor/", 10)));
        assertEquals(List.of("50%/x"), keys(state.list("50%/", 10)), "a prefix is matched literally");
        assertTrue(otherState.get("cursorX").isEmpty());

        assertThrows(IllegalArgumentException.class, () -> state.list("cursor/", 0));
        assertThrows(IllegalArgumentException.class,
                () -> state.list("cursor/", PostgresKeyValueStore.MAX_LIST_LIMIT + 1));
    }

    @Test
    void idempotentRepositoryRemembersSeenIdsUnderItsPrefixUntilTheyExpire() throws Exception {
        IdempotentRepository seen = state.idempotentRepository("seen/orders", Duration.ofDays(7));

        assertTrue(seen.add("order-1"));
        assertFalse(seen.add("order-1"));
        assertTrue(seen.contains("order-1"));
        assertFalse(seen.contains("order-2"));
        assertTrue(seen.confirm("order-1"));
        assertNotNull(state.get("seen/orders/order-1").orElseThrow().expiresAt());
        assertTrue(seen.remove("order-1"));
        assertFalse(seen.contains("order-1"));

        seen.add("order-3");
        expire(INTEGRATION, "seen/orders/order-3");
        assertTrue(seen.add("order-3"), "an expired id is new again");

        insertKeys(INTEGRATION, "seen/orders/", 2 * PostgresKeyValueStore.DELETE_BATCH_SIZE + 1, "null");
        state.put("seen/ordersX/1", BooleanNode.TRUE);
        otherState.put("seen/orders/order-3", BooleanNode.TRUE);
        seen.clear();

        assertTrue(state.list("seen/orders/", 10).isEmpty());
        assertEquals(List.of("seen/ordersX/1"), keys(state.list("seen/", 10)));
        assertTrue(otherState.get("seen/orders/order-3").isPresent());
        assertThrows(IllegalArgumentException.class, () -> state.idempotentRepository("seen/orders", null));
    }

    @Test
    void idempotentConsumerProcessesAnIdOnceAndAgainAfterAFailedAttempt() throws Exception {
        AtomicInteger processed = new AtomicInteger();
        AtomicBoolean failNext = new AtomicBoolean(true);
        IdempotentRepository seen = state.idempotentRepository("seen/files", Duration.ofDays(1));
        DefaultCamelContext context = new DefaultCamelContext();
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:poll-files")
                        .idempotentConsumer(header("fileName"), seen)
                        .process(exchange -> {
                            if (failNext.getAndSet(false)) {
                                throw new IllegalStateException("downstream unavailable");
                            }
                            processed.incrementAndGet();
                        });
            }
        });

        try {
            context.start();
            ProducerTemplate template = context.createProducerTemplate();

            assertThrows(CamelExecutionException.class,
                    () -> template.requestBodyAndHeader("direct:poll-files", "a", "fileName", "a.csv"));
            assertFalse(seen.contains("a.csv"), "a failed attempt forgets the id");
            template.requestBodyAndHeader("direct:poll-files", "a", "fileName", "a.csv");
            template.requestBodyAndHeader("direct:poll-files", "a", "fileName", "a.csv");

            assertEquals(1, processed.get());
            assertTrue(state.get("seen/files/a.csv").isPresent());
        } finally {
            context.stop();
        }
    }

    @Test
    void withoutTheTableTheSweepIdlesAndStateCallsNameTheContract() throws Exception {
        execute("drop table " + PostgresKeyValueStore.TABLE_NAME);
        IdempotentRepository seen = state.idempotentRepository("seen/files", Duration.ofDays(1));

        assertEquals(0, state.sweepExpired());

        KeyValueStoreException missing = assertThrows(KeyValueStoreException.class,
                () -> state.get("cursor/ledger"));
        assertTrue(missing.getMessage().contains("contract 1.2"), missing.getMessage());
        assertThrows(KeyValueStoreException.class, () -> state.put("cursor/ledger", BooleanNode.TRUE));
        assertThrows(KeyValueStoreException.class, () -> seen.add("a.csv"));
    }

    private void insertKeys(String integration, String prefix, int count, String expiresAt) throws Exception {
        execute("insert into bip_key_value (integration, key, value, expires_at) "
                + "select '" + integration + "', '" + prefix + "' || n, 'true', " + expiresAt
                + " from generate_series(1, " + count + ") n");
    }

    private void expire(String integration, String key) throws Exception {
        execute("update bip_key_value set expires_at = current_timestamp - interval '1 second' "
                + "where integration = '" + integration + "' and key = '" + key + "'");
    }

    private static List<String> keys(List<KeyValueKey> keys) {
        return keys.stream().map(KeyValueKey::key).toList();
    }

    private static JsonNode json(String json) throws Exception {
        return JSON.readTree(json);
    }

    private long count(String sql) throws Exception {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            resultSet.next();
            return resultSet.getLong(1);
        }
    }

    private void execute(String sql) throws Exception {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private DataSource dataSource() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setURL(POSTGRES.getJdbcUrl());
        dataSource.setUser(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        return dataSource;
    }

    private String schemaSql(String resource) throws Exception {
        try (InputStream input = getClass().getResourceAsStream((resource.equals("bip_key_value.sql") ? "/db/key-value/" : "/db/message-store/") + resource)) {
            if (input == null) {
                throw new AssertionError("Missing schema resource " + resource);
            }
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
