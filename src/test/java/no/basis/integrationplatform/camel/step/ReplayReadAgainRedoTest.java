package no.basis.integrationplatform.camel.step;

import no.basis.integrationplatform.camel.PlatformDeadLetterRoute;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.PlatformRouteConfiguration;
import no.basis.integrationplatform.camel.ReceiverComponent;
import no.basis.integrationplatform.camel.TestJdbc;
import no.basis.integrationplatform.camel.lifecycle.MessageStoreRoutePolicy;
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
import java.sql.Statement;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A resend's replay record that is read again before its attempt has an outcome, here because a
 * later step failed while parking was not possible, does not send its redo step again: one redo
 * per resend.
 */
@Testcontainers
class ReplayReadAgainRedoTest {
    @Container
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    private DataSource dataSource;
    private final ReceiverComponent receiver = new ReceiverComponent();
    private final AtomicInteger commits = new AtomicInteger();
    private final AtomicInteger offset = new AtomicInteger(7);
    private volatile boolean failAfterSend;
    private volatile boolean parkingFails;
    private DefaultCamelContext context;

    @BeforeEach
    void setUp() throws Exception {
        PGSimpleDataSource pg = new PGSimpleDataSource();
        pg.setURL(POSTGRES.getJdbcUrl());
        pg.setUser(POSTGRES.getUsername());
        pg.setPassword(POSTGRES.getPassword());
        dataSource = pg;
        TestJdbc.awaitReachable(dataSource);
        execute("drop table if exists bip_message_replay_audit, bip_message_attempt, bip_message_log, "
                + "bip_step_outcome cascade");
        for (String resource : new String[]{"bip_message_store.sql", "bip_step_outcome.sql"}) {
            try (InputStream in = getClass().getResourceAsStream("/db/message-store/" + resource)) {
                for (String sql : new String(in.readAllBytes(), StandardCharsets.UTF_8).split(";")) {
                    if (!sql.isBlank()) {
                        execute(sql);
                    }
                }
            }
        }
        context = new DefaultCamelContext();
        var properties = new Properties();
        properties.setProperty("basis.message-store.dlq-uri", "direct:parking");
        context.getPropertiesComponent().setOverrideProperties(properties);
        context.addComponent("receiver", receiver);
        context.getCamelContextExtension().addInterceptStrategy(
                new StepTrackingInterceptStrategy(new DefaultStepTracker(new PostgresStepOutcomeStore(dataSource))));
        context.addRoutes(new PlatformRouteConfiguration());
        context.addRoutes(new PlatformDeadLetterRoute());
        PostgresMessageStoreSink sink = new PostgresMessageStoreSink(dataSource);
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                from("direct:parking").routeId("parking").errorHandler(noErrorHandler()).process(e -> {
                    if (parkingFails) {
                        throw new IllegalStateException("DLQ unavailable");
                    }
                });
                from("direct:input").routeId("input").routeConfigurationId(PlatformRouteConfiguration.NO_RETRY)
                        .routePolicy(new MessageStoreRoutePolicy(sink, "order-pipeline"))
                        .to("receiver:orders").id("post-order")
                        .process(e -> {
                            if (failAfterSend) {
                                throw new IllegalStateException("a later step failed");
                            }
                        });
            }
        });
        context.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (context != null) {
            context.close();
        }
    }

    @Test
    void aReplayReadAgainAfterItsRedoWasSentDoesNotRedoItAgain() throws Exception {
        read("message-1", null);
        assertEquals(1, receiver.received().size());
        audit();
        offset.set(8);
        failAfterSend = true;
        parkingFails = true;
        read("message-1", "[\"post-order\"]");
        assertEquals(2, receiver.received().size(), "The resend's redo sent the step again, once");
        assertEquals("FAILED", value("select status from bip_message_attempt where message_id = 'message-1' and attempt = 2"));
        assertEquals(1, commits.get(), "The replay record is not committed");

        failAfterSend = false;
        parkingFails = false;
        read("message-1", "[\"post-order\"]");
        assertEquals("COMPLETED", value("select status from bip_message_attempt where message_id = 'message-1' and attempt = 2"));
        assertEquals(2, receiver.received().size(), "The redo was made in the earlier read of this record");
    }

    @Test
    void whileParkingFailsEveryReadOfTheReplayRecordSkipsTheRedoneStep() throws Exception {
        read("message-1", null);
        audit();
        offset.set(8);
        failAfterSend = true;
        parkingFails = true;
        for (int i = 0; i < 3; i++) {
            read("message-1", "[\"post-order\"]");
        }
        assertEquals(1, commits.get());
        assertEquals(2, receiver.received().size(), "One original send and one redo, however often the record is read");
    }

    private void audit() throws Exception {
        execute("insert into bip_message_replay_audit (replay_id, message_id, integration, replay_attempt, redo_steps,"
                + " target_topic, status, requested_at) values ('" + UUID.randomUUID() + "', 'message-1',"
                + " 'order-pipeline', 2, '[\"post-order\"]'::jsonb, 'bip.order-pipeline.in', 'PUBLISHED', now())");
    }

    private void read(String messageId, String redoSteps) {
        try (var producer = context.createProducerTemplate()) {
            producer.send("direct:input", e -> {
                e.getMessage().setHeader(KafkaConstants.TOPIC, "bip.order-pipeline.in");
                e.getMessage().setHeader(KafkaConstants.PARTITION, 0);
                e.getMessage().setHeader(KafkaConstants.OFFSET, (long) offset.get());
                e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, (KafkaManualCommit) commits::incrementAndGet);
                e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, messageId);
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
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(sql)) {
            return result.next() ? result.getString(1) : null;
        }
    }

    private void execute(String sql) throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
