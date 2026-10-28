package no.basis.integrationplatform.camel.lifecycle;

import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.step.DefaultStepTracker;
import no.basis.integrationplatform.camel.step.StepOutcomeStore;
import no.basis.integrationplatform.camel.step.StepTrackingInterceptStrategy;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A route that publishes to another recording integration sets a new x-bip-message-id
 * before its Kafka send. The adapter publishes exactly that value. The upstream's own
 * journal row keeps its input id only when the route does not leave the new id on the
 * exchange: the outcome is recorded from the message headers at completion.
 */
@Testcontainers
class PublishedMessageIdTest {
    @Container
    static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));
    private static final String INPUT_ID = "upstream-input-1";
    private static final String NEW_ID = "downstream-record-1";

    @ParameterizedTest(name = "{0}")
    @CsvSource({
            // pattern,      journaled upstream id, tracked publish
            "set-header,     " + NEW_ID + ",       true",
            "restore-header, " + INPUT_ID + ",     true",
            "multicast,      " + NEW_ID + ",       true",
            "new-exchange,   " + INPUT_ID + ",     false",
    })
    void publishesTheRouteSetIdAndJournalsUpstreamAsShown(String pattern, String journaledId, boolean tracked) throws Exception {
        String topic = "out-" + UUID.randomUUID();
        String brokers = KAFKA.getBootstrapServers();
        String kafka = "kafka:" + topic + "?brokers=" + brokers;
        List<MessageAttemptResult> rows = new CopyOnWriteArrayList<>();
        Set<String> ledger = java.util.Collections.synchronizedSet(new HashSet<>());
        StepOutcomeStore store = new StepOutcomeStore() {
            public boolean wasDelivered(String key) { return ledger.contains(key); }
            public void recordDelivered(StepResult step) { ledger.add(step.idempotencyKey()); }
            public void recordFailed(StepResult step) { }
        };
        try (var admin = AdminClient.create(Map.<String, Object>of("bootstrap.servers", brokers));
             var context = new DefaultCamelContext()) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(20, TimeUnit.SECONDS);
            context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(new DefaultStepTracker(store)));
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    var route = from("direct:upstream").routeId("upstream-input")
                            .routePolicy(new MessageStoreRoutePolicy(rows::add, "upstream"));
                    switch (pattern) {
                        case "set-header" -> route
                                .setHeader(PlatformHeaders.MESSAGE_ID, constant(NEW_ID))
                                .to(kafka).id("publish-downstream");
                        case "restore-header" -> route
                                .setProperty("upstreamMessageId", header(PlatformHeaders.MESSAGE_ID))
                                .setHeader(PlatformHeaders.MESSAGE_ID, constant(NEW_ID))
                                .to(kafka).id("publish-downstream")
                                .setHeader(PlatformHeaders.MESSAGE_ID, exchangeProperty("upstreamMessageId"));
                        case "multicast" -> route
                                .multicast()
                                    .pipeline()
                                        .setHeader(PlatformHeaders.MESSAGE_ID, constant(NEW_ID))
                                        .to(kafka).id("publish-downstream")
                                    .end()
                                .end();
                        case "new-exchange" -> route
                                .process(e -> {
                                    try (var producer = e.getContext().createProducerTemplate()) {
                                        producer.sendBodyAndHeaders(kafka, e.getMessage().getBody(String.class),
                                                Map.of(PlatformHeaders.MESSAGE_ID, NEW_ID,
                                                        PlatformHeaders.CORRELATION_ID, "order-1"));
                                    }
                                });
                        default -> throw new IllegalArgumentException(pattern);
                    }
                }
            });
            context.start();
            context.createProducerTemplate().send("direct:upstream", e -> {
                e.getMessage().setHeader(KafkaConstants.TOPIC, "bip.upstream.in");
                e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, (KafkaManualCommit) () -> { });
                e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, INPUT_ID);
                e.getMessage().setHeader(PlatformHeaders.CORRELATION_ID, "order-1");
                e.getMessage().setBody("<Order/>");
            });
        }

        var record = consumeOne(brokers, topic);
        assertEquals(NEW_ID, header(record, PlatformHeaders.MESSAGE_ID), "Published id");
        assertEquals("order-1", header(record, PlatformHeaders.CORRELATION_ID), "Continuity travels in the correlation id");
        assertEquals(tracked ? NEW_ID + ":publish-downstream" : null, header(record, PlatformHeaders.IDEMPOTENCY_KEY));
        assertEquals(1, rows.size());
        assertEquals(journaledId, rows.getFirst().messageId(), "Upstream journal row id");
    }

    /**
     * The handbook's example (message envelope, "Message ids between integrations"),
     * pasted verbatim as a sub-route of a recording Kafka input.
     */
    @org.junit.jupiter.api.Test
    void handbookExampleDerivesTheIdDeterministicallyAndRestoresTheInputId() throws Exception {
        String brokers = KAFKA.getBootstrapServers();
        List<MessageAttemptResult> rows = new CopyOnWriteArrayList<>();
        Set<String> ledger = java.util.Collections.synchronizedSet(new HashSet<>());
        StepOutcomeStore store = new StepOutcomeStore() {
            public boolean wasDelivered(String key) { return ledger.contains(key); }
            public void recordDelivered(StepResult step) { ledger.add(step.idempotencyKey()); }
            public void recordFailed(StepResult step) { }
        };
        try (var admin = AdminClient.create(Map.<String, Object>of("bootstrap.servers", brokers));
             var context = new DefaultCamelContext()) {
            admin.createTopics(List.of(new NewTopic("bip.orders.in", 1, (short) 1))).all().get(20, TimeUnit.SECONDS);
            context.getComponent("kafka", org.apache.camel.component.kafka.KafkaComponent.class)
                    .getConfiguration().setBrokers(brokers);
            context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(new DefaultStepTracker(store)));
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:upstream").routeId("upstream-input")
                            .routePolicy(new MessageStoreRoutePolicy(rows::add, "upstream"))
                            .to("direct:publish-orders");

                    // Verbatim from the handbook.
                    from("direct:publish-orders")
                        .setProperty("inputMessageId", header("x-bip-message-id"))
                        .process(exchange -> exchange.getMessage().setHeader("x-bip-message-id",
                            UUID.nameUUIDFromBytes((exchange.getProperty("inputMessageId", String.class)
                                + ":bip.orders.in").getBytes(StandardCharsets.UTF_8)).toString()))
                        .to("kafka:bip.orders.in").id("publish-orders")
                        .setHeader("x-bip-message-id", exchangeProperty("inputMessageId"));
                }
            });
            context.start();
            var producer = context.createProducerTemplate();
            for (String[] delivery : new String[][]{{null, null}, {null, null}, {"2", "[\"publish-orders\"]"}}) {
                producer.send("direct:upstream", e -> {
                    e.getMessage().setHeader(KafkaConstants.TOPIC, "bip.upstream.in");
                    e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, (KafkaManualCommit) () -> { });
                    e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, INPUT_ID);
                    e.getMessage().setHeader(PlatformHeaders.CORRELATION_ID, "order-1");
                    if (delivery[0] != null) {
                        e.getMessage().setHeader(PlatformHeaders.REPLAY, "true");
                        e.getMessage().setHeader(PlatformHeaders.ATTEMPT, delivery[0]);
                        e.getMessage().setHeader(PlatformHeaders.REDO_STEPS, delivery[1]);
                    }
                    e.getMessage().setBody("<Order/>");
                });
            }
        }

        String derived = UUID.nameUUIDFromBytes((INPUT_ID + ":bip.orders.in").getBytes(StandardCharsets.UTF_8)).toString();
        // First consumption publishes; consuming the same record again skips the tracked send;
        // a resend with redo publishes again under the same derived id.
        var published = consumeAll(brokers, "bip.orders.in", 2);
        assertEquals(List.of(derived, derived), published.stream().map(r -> header(r, PlatformHeaders.MESSAGE_ID)).toList());
        assertEquals(List.of("order-1", "order-1"), published.stream().map(r -> header(r, PlatformHeaders.CORRELATION_ID)).toList());
        assertEquals(derived + ":publish-orders", header(published.getFirst(), PlatformHeaders.IDEMPOTENCY_KEY));
        assertEquals(List.of(INPUT_ID, INPUT_ID, INPUT_ID), rows.stream().map(MessageAttemptResult::messageId).toList());
        assertEquals(List.of(1, 1, 2), rows.stream().map(MessageAttemptResult::attempt).toList());
        assertEquals(List.of("DELIVERED", "SKIPPED", "DELIVERED"),
                rows.stream().map(r -> r.steps().getFirst().status().name()).toList());
    }

    /**
     * The send with the derived id fails after its retries, so the restore after it never
     * runs. The parked record and the upstream journal row must still use the input id.
     */
    @org.junit.jupiter.api.Test
    void failedPublishIsParkedAndJournaledUnderTheInputId() throws Exception {
        List<MessageAttemptResult> rows = new CopyOnWriteArrayList<>();
        List<Object> parkedIds = new CopyOnWriteArrayList<>();
        var broker = new no.basis.integrationplatform.camel.ReceiverComponent(e -> {
            throw new IllegalStateException("broker unavailable");
        });
        try (var context = new DefaultCamelContext()) {
            var properties = new java.util.Properties();
            properties.setProperty("basis.message-store.dlq-uri", "direct:parking");
            context.getPropertiesComponent().setOverrideProperties(properties);
            context.addComponent("broker", broker);
            context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(
                    new DefaultStepTracker(new no.basis.integrationplatform.camel.step.NoopStepOutcomeStore())));
            context.addRoutes(new no.basis.integrationplatform.camel.PlatformRouteConfiguration());
            context.addRoutes(new no.basis.integrationplatform.camel.PlatformDeadLetterRoute());
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:parking").errorHandler(noErrorHandler())
                            .process(e -> parkedIds.add(e.getMessage().getHeader(PlatformHeaders.MESSAGE_ID)));
                    from("direct:upstream").routeConfigurationId(no.basis.integrationplatform.camel.PlatformRouteConfiguration.NO_RETRY)
                            .routePolicy(new MessageStoreRoutePolicy(rows::add, "upstream"))
                            .to("direct:publish-orders");
                    from("direct:publish-orders")
                        .setProperty("inputMessageId", header("x-bip-message-id"))
                        .process(exchange -> exchange.getMessage().setHeader("x-bip-message-id",
                            UUID.nameUUIDFromBytes((exchange.getProperty("inputMessageId", String.class)
                                + ":bip.orders.in").getBytes(StandardCharsets.UTF_8)).toString()))
                        .to("broker:bip.orders.in").id("publish-orders")
                        .setHeader("x-bip-message-id", exchangeProperty("inputMessageId"));
                }
            });
            context.start();
            context.createProducerTemplate().send("direct:upstream", e -> {
                e.getMessage().setHeader(KafkaConstants.TOPIC, "bip.upstream.in");
                e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, (KafkaManualCommit) () -> { });
                e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, INPUT_ID);
                e.getMessage().setBody("<Order/>");
            });
        }
        String derived = UUID.nameUUIDFromBytes((INPUT_ID + ":bip.orders.in").getBytes(StandardCharsets.UTF_8)).toString();
        assertEquals(derived, broker.received().getFirst().getMessage().getHeader(PlatformHeaders.MESSAGE_ID),
                "The failed send itself carried the derived id");
        assertEquals(List.of(INPUT_ID), parkedIds);
        assertEquals(List.of(INPUT_ID + "/DLQ"), rows.stream().map(r -> r.messageId() + "/" + r.status()).toList());
    }

    private static List<ConsumerRecord<String, String>> consumeAll(String brokers, String topic, int expected) {
        try (var consumer = new KafkaConsumer<String, String>(Map.of("bootstrap.servers", brokers,
                "group.id", "verify-" + UUID.randomUUID(), "auto.offset.reset", "earliest"),
                new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(topic));
            var records = new ArrayList<ConsumerRecord<String, String>>();
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (records.size() < expected && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(200)).forEach(records::add);
            }
            // Give a late extra record a moment to show up.
            consumer.poll(Duration.ofSeconds(2)).forEach(records::add);
            assertEquals(expected, records.size(), "Published records");
            return records;
        }
    }

    private static ConsumerRecord<String, String> consumeOne(String brokers, String topic) {
        try (var consumer = new KafkaConsumer<String, String>(Map.of("bootstrap.servers", brokers,
                "group.id", "verify-" + UUID.randomUUID(), "auto.offset.reset", "earliest"),
                new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(topic));
            var records = new ArrayList<ConsumerRecord<String, String>>();
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (records.isEmpty() && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(200)).forEach(records::add);
            }
            assertEquals(1, records.size(), "Exactly one published record");
            return records.getFirst();
        }
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        var header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
