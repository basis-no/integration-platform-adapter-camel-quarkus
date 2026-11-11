package no.basis.integrationplatform.camel.step;

import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.ReceiverComponent;
import no.basis.integrationplatform.camel.TestJdbc;
import no.basis.integrationplatform.camel.lifecycle.MessageStoreRoutePolicy;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.store.PostgresMessageStoreSink;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A Message Store that can be read but not written: a send that returned is not sent again on
 * the record's next reads only because its DELIVERED outcome could not be written (#137).
 */
@Testcontainers
class StoreWriteFailureTest {
    @Container
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");
    private static final String APP = "adapter_app";

    private DataSource owner;
    private DataSource app;
    private final ReceiverComponent receiver = new ReceiverComponent();
    private final AtomicInteger commits = new AtomicInteger();
    private DefaultCamelContext context;

    @BeforeEach
    void setUp() throws Exception {
        owner = dataSource(POSTGRES.getUsername(), POSTGRES.getPassword());
        TestJdbc.awaitReachable(owner);
        execute("drop table if exists bip_message_replay_audit, bip_message_attempt, bip_message_log, bip_step_outcome cascade");
        for (String resource : new String[]{"bip_message_store.sql", "bip_step_outcome.sql"}) {
            try (InputStream in = getClass().getResourceAsStream("/db/message-store/" + resource)) {
                for (String sql : new String(in.readAllBytes(), StandardCharsets.UTF_8).split(";")) {
                    if (!sql.isBlank()) {
                        execute(sql);
                    }
                }
            }
        }
        execute("do $$ begin if not exists (select from pg_roles where rolname = '" + APP + "') then "
                + "create role " + APP + " login password 'app-test-only'; end if; end $$");
        execute("alter role " + APP + " reset default_transaction_read_only");
        execute("grant select, insert, update, delete on all tables in schema public to " + APP);
        app = dataSource(APP, "app-test-only");
    }

    @AfterEach
    void tearDown() throws Exception {
        if (context != null) {
            context.close();
        }
        execute("alter role " + APP + " reset default_transaction_read_only");
    }

    @Test
    void aReadOnlyStoreSendsOnceAndTheMessageCompletesWithoutASecondSend() throws Exception {
        start(new PostgresStepOutcomeStore(app));
        execute("alter role " + APP + " set default_transaction_read_only = on");

        read("message-1");
        read("message-1");
        read("message-1");
        assertEquals(1, receiver.received().size(), "One send, not one per read");
        assertEquals(0, commits.get());

        execute("alter role " + APP + " reset default_transaction_read_only");
        read("message-1");

        assertEquals(1, receiver.received().size(), "The outcome is written from the mark, not sent again");
        assertEquals(1, commits.get());
        assertEquals("COMPLETED", value("select status from bip_message_attempt where message_id = 'message-1'"));
        assertEquals("DELIVERED", value("select status from bip_step_outcome where idempotency_key = 'message-1:post-order'"));
        assertEquals("DELIVERED", value("select steps_json->0->>'status' from bip_message_attempt where message_id = 'message-1'"));
    }

    @Test
    void aWriteThatTimesOutOnAHeldLockIsMarkedAndNotSentAgain() throws Exception {
        start(new PostgresStepOutcomeStore(app, no.basis.integrationplatform.camel.MessageStoreTimeouts.of(
                java.time.Duration.ofSeconds(2), java.time.Duration.ofMillis(300))));
        try (Connection holder = owner.getConnection()) {
            holder.setAutoCommit(false);
            try (Statement statement = holder.createStatement()) {
                // An uncommitted row with the same step key: the DELIVERED upsert waits on it.
                statement.execute("insert into bip_step_outcome (idempotency_key, message_id, step_id, status, "
                        + "occurred_at, updated_at) values ('message-1:post-order', 'message-1', 'post-order', "
                        + "'FAILED', now(), now())");
            }
            try (var logs = no.basis.integrationplatform.camel.LogCapture.of(
                    no.basis.integrationplatform.camel.lifecycle.MessageStoreRoutePolicy.class)) {
                read("message-1");

                assertEquals(1, receiver.received().size());
                assertEquals(0, commits.get());
                assertTrue(logs.at(java.util.logging.Level.SEVERE).stream()
                        .map(no.basis.integrationplatform.camel.LogCapture::text)
                        .anyMatch(line -> line.contains("storeError=unavailable sqlState=55P03")));
            }
            holder.rollback();
        }

        read("message-1");

        assertEquals(1, receiver.received().size(), "The outcome is written from the mark, not sent again");
        assertEquals(1, commits.get());
        assertEquals("DELIVERED", value("select status from bip_step_outcome where idempotency_key = 'message-1:post-order'"));
    }

    @Test
    void aLostWriteGrantSendsOnce() throws Exception {
        start(new PostgresStepOutcomeStore(app));
        execute("revoke insert, update on bip_step_outcome from " + APP);

        read("message-1");
        read("message-1");
        assertEquals(1, receiver.received().size());
        assertEquals("FAILED", value("select status from bip_message_attempt where message_id = 'message-1'"));

        execute("grant insert, update on bip_step_outcome to " + APP);
        read("message-1");

        assertEquals(1, receiver.received().size());
        assertEquals("COMPLETED", value("select status from bip_message_attempt where message_id = 'message-1'"));
        assertEquals(1, commits.get());
    }

    @Test
    void aRecordWithoutAMessageIdKeepsItsGeneratedIdAndSendsOnce() throws Exception {
        start(new PostgresStepOutcomeStore(app));
        execute("alter role " + APP + " set default_transaction_read_only = on");

        read(null);
        read(null);
        execute("alter role " + APP + " reset default_transaction_read_only");
        read(null);

        assertEquals(1, receiver.received().size());
        String id = receiver.received().getFirst().getMessage().getHeader(PlatformHeaders.MESSAGE_ID, String.class);
        assertEquals(id, value("select message_id from bip_message_attempt where status = 'COMPLETED'"));
        assertEquals(id + ":post-order", value("select idempotency_key from bip_step_outcome where status = 'DELIVERED'"));
    }

    @Test
    void aWriteThatFailsOnceIsTriedAgainInPlace() throws Exception {
        AtomicInteger failures = new AtomicInteger(1);
        PostgresStepOutcomeStore postgres = new PostgresStepOutcomeStore(app);
        start(new StepOutcomeStore() {
            public boolean wasDelivered(String key) { return postgres.wasDelivered(key); }
            public void recordFailed(StepResult step) { postgres.recordFailed(step); }
            public void recordDelivered(StepResult step) {
                if (failures.getAndDecrement() > 0) {
                    throw new StepOutcomeStoreException("synthetic", new SQLException("connection reset", "08006"));
                }
                postgres.recordDelivered(step);
            }
        });

        read("message-1");

        assertEquals(1, receiver.received().size());
        assertEquals(1, commits.get(), "Committed on the first read");
        assertEquals("COMPLETED", value("select status from bip_message_attempt where message_id = 'message-1'"));
    }

    @Test
    void aResendWithRedoAfterTheStoreIsBackStillSends() throws Exception {
        start(new PostgresStepOutcomeStore(app));
        execute("alter role " + APP + " set default_transaction_read_only = on");
        read("message-1");
        execute("alter role " + APP + " reset default_transaction_read_only");
        read("message-1");
        assertEquals(1, receiver.received().size());

        // A resend is a new record, at another offset.
        offset.set(8);
        send("message-1", "[\"post-order\"]");

        assertEquals(2, receiver.received().size(), "The mark belonged to the first record only");
    }

    @Test
    void aRecordWhoseCorrelationIdTheStoreCannotHoldIsParkedAndTheNextOneIsProcessed() throws Exception {
        start(new PostgresStepOutcomeStore(app));

        sendWithCorrelation("message-1", "order\u0000-1");
        offset.set(8);
        sendWithCorrelation("message-2", "order-2");

        assertEquals(1, parked.size());
        assertEquals("DLQ", value("select status from bip_message_log where message_id = 'message-1'"));
        assertEquals(null, value("select correlation_id from bip_message_log where message_id = 'message-1'"));
        assertEquals("COMPLETED", value("select status from bip_message_log where message_id = 'message-2'"));
        assertEquals(1, receiver.received().size(), "Only the next record was sent");
        assertEquals(2, commits.get());
    }

    @Test
    void anIdTheRouteSetsThatTheStepLedgerCannotIndexParksTheRecordAfterOneSend() throws Exception {
        // Random, so that the index cannot compress it below its 2704-byte limit.
        byte[] random = new byte[2_400];
        new java.util.Random(7).nextBytes(random);
        routeSetsId = java.util.HexFormat.of().formatHex(random);
        start(new PostgresStepOutcomeStore(app));

        read("message-1");

        assertEquals(1, receiver.received().size());
        assertEquals(1, parked.size());
        String error = value("select error from bip_message_attempt where message_id = 'message-1'");
        assertTrue(error.contains("SQLSTATE 54000") && error.contains("the send of step post-order had returned"), error);
        assertEquals(1, commits.get());
    }

    private final AtomicInteger offset = new AtomicInteger(7);
    private final java.util.List<org.apache.camel.Exchange> parked = new java.util.concurrent.CopyOnWriteArrayList<>();
    private volatile String routeSetsId;

    private void start(StepOutcomeStore store) throws Exception {
        DefaultStepTracker tracker = new DefaultStepTracker(store);
        tracker.setRetrySleeper(millis -> true);
        context = new DefaultCamelContext();
        var properties = new java.util.Properties();
        properties.setProperty("basis.message-store.dlq-uri", "direct:parking");
        context.getPropertiesComponent().setOverrideProperties(properties);
        context.addRoutes(new no.basis.integrationplatform.camel.PlatformDeadLetterRoute());
        context.addComponent("receiver", receiver);
        context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(tracker));
        PostgresMessageStoreSink sink = new PostgresMessageStoreSink(app);
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                from("direct:parking").routeId("parking").errorHandler(noErrorHandler()).process(e -> parked.add(e.copy()));
                from("direct:input").routeId("input").errorHandler(noErrorHandler())
                        .routePolicy(new MessageStoreRoutePolicy(sink, "order-pipeline"))
                        .process(e -> {
                            if (routeSetsId != null) {
                                e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, routeSetsId);
                            }
                        })
                        .to("receiver:orders").id("post-order");
            }
        });
        context.start();
    }

    private void read(String messageId) {
        send(messageId, null);
    }

    private void sendWithCorrelation(String messageId, String correlationId) {
        try (var producer = context.createProducerTemplate()) {
            producer.send("direct:input", e -> {
                e.getMessage().setHeader(KafkaConstants.TOPIC, "bip.order-pipeline.in");
                e.getMessage().setHeader(KafkaConstants.PARTITION, 0);
                e.getMessage().setHeader(KafkaConstants.OFFSET, (long) offset.get());
                e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, (KafkaManualCommit) commits::incrementAndGet);
                e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, messageId);
                e.getMessage().setHeader(PlatformHeaders.CORRELATION_ID, correlationId.getBytes(StandardCharsets.UTF_8));
                e.getMessage().setBody("<Order/>");
            });
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private void send(String messageId, String redoSteps) {
        try (var producer = context.createProducerTemplate()) {
            producer.send("direct:input", e -> {
                e.getMessage().setHeader(KafkaConstants.TOPIC, "bip.order-pipeline.in");
                e.getMessage().setHeader(KafkaConstants.PARTITION, 0);
                e.getMessage().setHeader(KafkaConstants.OFFSET, (long) offset.get());
                e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, (KafkaManualCommit) commits::incrementAndGet);
                if (messageId != null) {
                    e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, messageId);
                }
                if (redoSteps != null) {
                    e.getMessage().setHeader(PlatformHeaders.REPLAY, "true");
                    e.getMessage().setHeader(PlatformHeaders.ATTEMPT, "2");
                    e.getMessage().setHeader(PlatformHeaders.REDO_STEPS, redoSteps);
                }
                e.getMessage().setBody("<Order/>");
            });
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private String value(String sql) throws Exception {
        try (Connection connection = owner.getConnection(); Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(sql)) {
            return result.next() ? result.getString(1) : null;
        }
    }

    private void execute(String sql) throws Exception {
        try (Connection connection = owner.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static DataSource dataSource(String user, String password) {
        PGSimpleDataSource source = new PGSimpleDataSource();
        source.setURL(POSTGRES.getJdbcUrl());
        source.setUser(user);
        source.setPassword(password);
        return source;
    }
}
