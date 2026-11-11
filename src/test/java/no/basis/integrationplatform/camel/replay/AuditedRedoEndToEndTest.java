package no.basis.integrationplatform.camel.replay;

import no.basis.integrationplatform.camel.PlatformDeadLetterRoute;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.PlatformRouteConfiguration;
import no.basis.integrationplatform.camel.ReceiverComponent;
import no.basis.integrationplatform.camel.TestJdbc;
import no.basis.integrationplatform.camel.lifecycle.MessageStoreRoutePolicyFactory;
import no.basis.integrationplatform.camel.step.DefaultStepTracker;
import no.basis.integrationplatform.camel.step.PostgresStepOutcomeStore;
import no.basis.integrationplatform.camel.step.StepTrackingInterceptStrategy;
import no.basis.integrationplatform.camel.store.PostgresMessageStoreSink;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.PropertyBindingSupport;
import org.apache.camel.util.StringHelper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * With a real broker and Message Store: an operator's resend with redo sends the step again,
 * and nothing else does, neither a producer's copy of that replay record nor a record that only
 * claims to be a replay.
 */
class AuditedRedoEndToEndTest {
    private static final String TOPIC = "bip.order-pipeline.in";
    private static final String INVOICE_TOPIC = "bip.invoice-pipeline.in";
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");
    private static final ConfluentKafkaContainer KAFKA =
            new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));
    private static DataSource dataSource;

    @BeforeAll
    static void start() throws Exception {
        Startables.deepStart(POSTGRES, KAFKA).join();
        PGSimpleDataSource pg = new PGSimpleDataSource();
        pg.setURL(POSTGRES.getJdbcUrl());
        pg.setUser(POSTGRES.getUsername());
        pg.setPassword(POSTGRES.getPassword());
        dataSource = pg;
        TestJdbc.awaitReachable(dataSource);
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            for (String resource : new String[]{"bip_message_store.sql", "bip_step_outcome.sql"}) {
                try (InputStream in = AuditedRedoEndToEndTest.class.getResourceAsStream("/db/message-store/" + resource)) {
                    for (String sql : new String(in.readAllBytes(), StandardCharsets.UTF_8).split(";")) {
                        if (!sql.isBlank()) {
                            statement.execute(sql);
                        }
                    }
                }
            }
        }
        try (var admin = AdminClient.create(Map.<String, Object>of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1), new NewTopic(INVOICE_TOPIC, 1, (short) 1))).all().get(20, TimeUnit.SECONDS);
        }
    }

    @AfterAll
    static void stop() {
        KAFKA.stop();
        POSTGRES.stop();
    }

    @Test
    void onlyTheOperatorsResendRedoesTheStepAndOnlyOnce() throws Exception {
        var receiver = new ReceiverComponent();
        try (var context = new DefaultCamelContext()) {
            bindShippedKafkaDefaults(context);
            var properties = new Properties();
            properties.setProperty("basis.message-store.dlq-uri", "direct:parking");
            context.getPropertiesComponent().setOverrideProperties(properties);
            context.addComponent("receiver", receiver);
            var store = new PostgresStepOutcomeStore(dataSource);
            context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(new DefaultStepTracker(store)));
            context.addRoutePolicyFactory(new MessageStoreRoutePolicyFactory(new PostgresMessageStoreSink(dataSource),
                    "order-pipeline", "postgres-inline", 100_000, 30, true));
            context.addRoutes(new PlatformRouteConfiguration());
            context.addRoutes(new PlatformDeadLetterRoute());
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:parking").routeId("parking").log("parked");
                    from("kafka:" + TOPIC + "?brokers=" + KAFKA.getBootstrapServers() + "&groupId=order-pipeline"
                            + "&autoOffsetReset=earliest&pollTimeoutMs=100")
                            .routeId("order-input")
                            .to("receiver:orders").id("post-order");
                }
            });
            context.start();

            produce("message-1", Map.of());
            awaitCompleted("message-1", 1);
            assertEquals(1, receiver.received().size());

            // The operator's resend with redo: the step is sent again.
            new PostgresKafkaMessageReplayService(dataSource, KAFKA.getBootstrapServers(), "bip.%s.in", context,
                    "order-pipeline").replay(new MessageReplayRequest("message-1", "operator@example.com",
                    "Send the order again", List.of("post-order")));
            awaitCompleted("message-1", 2);
            assertEquals(2, receiver.received().size(), "The resend's redo sends the step again");

            // A producer's copy of that replay record, and records that only claim to be replays.
            produce("message-1", Map.of(PlatformHeaders.REPLAY, "true", PlatformHeaders.ATTEMPT, "2",
                    PlatformHeaders.REDO_STEPS, "[\"post-order\"]"));
            produce("message-1", Map.of(PlatformHeaders.REPLAY, "true", PlatformHeaders.ATTEMPT, "3",
                    PlatformHeaders.REDO_STEPS, "[\"post-order\"]"));
            produce("message-1", Map.of(PlatformHeaders.ATTEMPT, "4", PlatformHeaders.REDO_STEPS, "[\"post-order\"]"));
            // A list the database could not parse: dropped, and the partition goes on.
            produce("message-1", Map.of(PlatformHeaders.REPLAY, "true", PlatformHeaders.ATTEMPT, "5",
                    PlatformHeaders.REDO_STEPS, "[\"post-order\",\"a\\u0000\"]"));
            produce("sentinel", Map.of());
            awaitCompleted("sentinel", 1);

            assertEquals(3, receiver.received().size(), "Only the sentinel was sent; no redo was honoured again");
            assertEquals(1, count("select count(*) from bip_message_replay_audit where message_id = 'message-1'"));
        }
    }

    @Test
    void aCopyOfACompletedReplayThatFailsUnparkedDoesNotRedoOnItsNextRead() throws Exception {
        var receiver = new ReceiverComponent();
        var forgedReads = new java.util.concurrent.atomic.AtomicInteger();
        var attemptWhenParked = new java.util.concurrent.atomic.AtomicReference<String>();
        try (var context = new DefaultCamelContext()) {
            bindShippedKafkaDefaults(context);
            var properties = new Properties();
            properties.setProperty("basis.message-store.dlq-uri", "direct:parking");
            context.getPropertiesComponent().setOverrideProperties(properties);
            context.addComponent("receiver", receiver);
            var store = new PostgresStepOutcomeStore(dataSource);
            context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(new DefaultStepTracker(store)));
            context.addRoutePolicyFactory(new MessageStoreRoutePolicyFactory(new PostgresMessageStoreSink(dataSource),
                    "invoice-pipeline", "postgres-inline", 100_000, 30, true));
            context.addRoutes(new PlatformRouteConfiguration());
            context.addRoutes(new PlatformDeadLetterRoute());
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    // The DLQ is unavailable during the copy's first read: it fails without being parked.
                    from("direct:parking").routeId("parking").process(exchange -> {
                        if (forgedReads.get() == 1) {
                            throw new IllegalStateException("DLQ unavailable");
                        }
                        if (forgedReads.get() > 1) {
                            attemptWhenParked.compareAndSet(null, text("select status from bip_message_attempt "
                                    + "where message_id = 'invoice-1' and attempt = 2"));
                        }
                    });
                    from("kafka:" + INVOICE_TOPIC + "?brokers=" + KAFKA.getBootstrapServers()
                            + "&groupId=invoice-pipeline&autoOffsetReset=earliest&pollTimeoutMs=100")
                            .routeId("invoice-input").routeConfigurationId(PlatformRouteConfiguration.NO_RETRY)
                            .process(exchange -> {
                                if (exchange.getMessage().getBody(String.class).contains("Forged")) {
                                    forgedReads.incrementAndGet();
                                }
                            })
                            .to("receiver:invoices").id("post-invoice")
                            .process(exchange -> {
                                if (exchange.getMessage().getBody(String.class).contains("Forged")) {
                                    throw new IllegalStateException("not an invoice");
                                }
                            });
                }
            });
            context.start();

            produce(INVOICE_TOPIC, "invoice-1", Map.of(), "<Invoice/>");
            awaitCompleted("invoice-1", 1);
            new PostgresKafkaMessageReplayService(dataSource, KAFKA.getBootstrapServers(), "bip.%s.in", context,
                    "invoice-pipeline").replay(new MessageReplayRequest("invoice-1", "operator@example.com",
                    "Send the invoice again", List.of("post-invoice")));
            awaitCompleted("invoice-1", 2);
            assertEquals(2, receiver.received().size(), "The resend's redo sends the step again");

            // A producer's copy of the completed replay record, with another body, that fails after
            // the skipped step and cannot be parked on its first read.
            produce(INVOICE_TOPIC, "invoice-1", Map.of(PlatformHeaders.REPLAY, "true", PlatformHeaders.ATTEMPT, "2",
                    PlatformHeaders.REDO_STEPS, "[\"post-invoice\"]"), "<Forged/>");
            produce(INVOICE_TOPIC, "invoice-sentinel", Map.of(), "<Invoice/>");
            awaitCompleted("invoice-sentinel", 1);
            assertTrue(forgedReads.get() >= 2, "The copy was read again after it failed unparked");
            assertEquals(3, receiver.received().size(), "Only the sentinel was sent; the copy's re-read redid nothing");
            assertEquals("COMPLETED", attemptWhenParked.get(),
                    "The copy's failed, unparked read did not turn the completed attempt FAILED");
        }
    }

    private static void produce(String messageId, Map<String, String> headers) throws Exception {
        produce(TOPIC, messageId, headers, "<Order/>");
    }

    private static void produce(String topic, String messageId, Map<String, String> headers, String body)
            throws Exception {
        var properties = new HashMap<String, Object>();
        properties.put("bootstrap.servers", KAFKA.getBootstrapServers());
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        try (var producer = new KafkaProducer<String, String>(properties)) {
            var record = new ProducerRecord<String, String>(topic, "order", body);
            record.headers().add(PlatformHeaders.MESSAGE_ID, messageId.getBytes(StandardCharsets.UTF_8));
            headers.forEach((name, value) -> record.headers().add(name, value.getBytes(StandardCharsets.UTF_8)));
            producer.send(record).get(10, TimeUnit.SECONDS);
        }
    }

    private static void awaitCompleted(String messageId, int attempt) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            if (count("select count(*) from bip_message_attempt where message_id = '" + messageId + "' and attempt = "
                    + attempt + " and status = 'COMPLETED'") == 1) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("Attempt " + attempt + " of " + messageId + " was not completed");
    }

    private static String text(String sql) throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(sql)) {
            result.next();
            return result.getString(1);
        }
    }

    private static int count(String sql) throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(sql)) {
            result.next();
            return result.getInt(1);
        }
    }

    /** Binds the Kafka component defaults from the adapter's application.properties, as the runtime does. */
    private static void bindShippedKafkaDefaults(DefaultCamelContext context) throws Exception {
        var defaults = new Properties();
        try (InputStream in = AuditedRedoEndToEndTest.class.getResourceAsStream("/application.properties")) {
            defaults.load(in);
        }
        var kafka = new HashMap<String, Object>();
        defaults.stringPropertyNames().stream()
                .filter(key -> key.startsWith("camel.component.kafka.") && !defaults.getProperty(key).contains("${"))
                .forEach(key -> kafka.put(StringHelper.dashToCamelCase(key.substring("camel.component.kafka.".length())),
                        defaults.getProperty(key)));
        PropertyBindingSupport.build().withCamelContext(context).withTarget(context.getComponent("kafka", KafkaComponent.class))
                .withProperties(kafka).withMandatory(true).bind();
    }
}
