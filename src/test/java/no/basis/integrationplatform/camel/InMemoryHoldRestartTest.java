package no.basis.integrationplatform.camel;

import no.basis.integrationplatform.camel.lifecycle.MessageStoreRoutePolicy;
import org.apache.camel.AggregationStrategy;
import org.apache.camel.builder.Builder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.model.ProcessorDefinition;
import org.apache.camel.model.RouteDefinition;
import org.apache.camel.support.PropertyBindingSupport;
import org.apache.camel.util.StringHelper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Camel's in-memory {@code aggregate} and {@code resequence} hold a record after its input
 * exchange has completed. Under the adapter's commit rule that input is then committed, so
 * a restart before the held record is released loses it: it is neither delivered nor read
 * again. This is why the adapter refuses such a hold on a Kafka boundary route
 * ({@code InMemoryHoldGuardTest}); here the adapter's Kafka policy is attached directly.
 */
@Testcontainers
class InMemoryHoldRestartTest {
    @Container
    static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    enum Hold {
        AGGREGATE((route, timeout) -> route.aggregate(Builder.header("group"), concatenate())
                .completionSize(10).completionTimeout(timeout)),
        BATCH_RESEQUENCE((route, timeout) -> route.resequence(Builder.header("seq")).batch().size(10).timeout(timeout)),
        STREAM_RESEQUENCE((route, timeout) -> route.resequence(Builder.header("seq")).stream().timeout(timeout));

        private final BiFunction<RouteDefinition, Long, ProcessorDefinition<?>> hold;

        Hold(BiFunction<RouteDefinition, Long, ProcessorDefinition<?>> hold) {
            this.hold = hold;
        }
    }

    @ParameterizedTest
    @EnumSource(Hold.class)
    void aHeldRecordIsLostWhenTheApplicationRestartsAfterItsInputWasCommitted(Hold hold) throws Exception {
        String input = "input-" + UUID.randomUUID();
        String group = "group-" + UUID.randomUUID();
        String brokers = KAFKA.getBootstrapServers();
        var client = Map.<String, Object>of("bootstrap.servers", brokers);
        try (var admin = AdminClient.create(client)) {
            admin.createTopics(List.of(new NewTopic(input, 1, (short) 1))).all().get(20, TimeUnit.SECONDS);
            var read = new CopyOnWriteArrayList<String>();
            var delivered = new CopyOnWriteArrayList<String>();
            var outcomes = new CopyOnWriteArrayList<String>();

            // First run: two records reach the hold, which waits a minute (and for ten records,
            // or for sequence 1) before it releases anything.
            send(input, "a", 2, "b", 3);
            try (var context = context(input, group, brokers, hold, 60_000, read, delivered, outcomes)) {
                context.start();
                awaitCommitted(admin, group, input, 2);
                assertEquals(List.of("a", "b"), read);
                assertEquals(List.of("COMPLETED", "COMPLETED"), outcomes,
                        "The inputs complete while their records are still held");
                assertEquals(List.of(), delivered, "Nothing is released before the restart");
            }

            // Second run, same group, after a restart, with a one-second timeout, so that the hold
            // releases what it has.
            read.clear();
            outcomes.clear();
            send(input, "c", 1, "d", 4);
            try (var context = context(input, group, brokers, hold, 1_000, read, delivered, outcomes)) {
                context.start();
                awaitCommitted(admin, group, input, 4);
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
                while (System.nanoTime() < deadline && !String.join(",", delivered).contains("d")) Thread.sleep(100);
            }

            assertEquals(List.of("c", "d"), read, "The committed records are not read again");
            String released = String.join(",", delivered);
            assertTrue(released.contains("c") && released.contains("d"), "The hold releases after the restart: " + delivered);
            assertTrue(!released.contains("a") && !released.contains("b"),
                    "The records held before the restart are lost: " + delivered);
        }
    }

    private static DefaultCamelContext context(String input, String group, String brokers, Hold hold, long timeout,
            List<String> read, List<String> delivered, List<String> outcomes) throws Exception {
        var context = new DefaultCamelContext();
        context.getShutdownStrategy().setTimeout(5);
        bindShippedKafkaDefaults(context);
        var properties = new Properties();
        properties.setProperty("basis.message-store.dlq-uri", "direct:parking");
        context.getPropertiesComponent().setOverrideProperties(properties);
        context.addRoutes(new PlatformRouteConfiguration());
        context.addRoutes(new PlatformDeadLetterRoute());
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                RouteDefinition route = from("kafka:" + input + "?brokers=" + brokers + "&groupId=" + group
                        + "&autoOffsetReset=earliest&pollTimeoutMs=100")
                        .routeId("input")
                        // The adapter's Kafka policy, attached directly: the automatic one refuses these routes.
                        .routePolicy(new MessageStoreRoutePolicy(row -> outcomes.add(row.status().name()), "order-pipeline"))
                        .process(e -> read.add(e.getMessage().getBody(String.class)))
                        .setHeader("group", constant("all"));
                hold.hold.apply(route, timeout).process(e -> delivered.add(e.getMessage().getBody(String.class)));
                from("direct:parking").routeId("parking").log("parked");
            }
        });
        return context;
    }

    private static AggregationStrategy concatenate() {
        return (oldExchange, newExchange) -> {
            if (oldExchange == null) return newExchange;
            oldExchange.getMessage().setBody(oldExchange.getMessage().getBody(String.class) + ","
                    + newExchange.getMessage().getBody(String.class));
            return oldExchange;
        };
    }

    /** Sends two records with their sequence numbers in the {@code seq} header. */
    private static void send(String topic, String first, int firstSeq, String second, int secondSeq) throws Exception {
        var properties = new HashMap<String, Object>();
        properties.put("bootstrap.servers", KAFKA.getBootstrapServers());
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        try (var producer = new KafkaProducer<String, String>(properties)) {
            for (var record : List.of(Map.entry(first, firstSeq), Map.entry(second, secondSeq))) {
                var produced = new ProducerRecord<String, String>(topic, "key", record.getKey());
                produced.headers().add("seq", Long.toString(record.getValue()).getBytes());
                producer.send(produced).get(10, TimeUnit.SECONDS);
            }
        }
    }

    private static void awaitCommitted(AdminClient admin, String group, String topic, long expected) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        long offset = -1;
        while (System.nanoTime() < deadline && offset != expected) {
            var position = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS)
                    .get(new TopicPartition(topic, 0));
            if (position != null) offset = position.offset();
            if (offset != expected) Thread.sleep(100);
        }
        assertEquals(expected, offset, "Committed offset");
    }

    private static void bindShippedKafkaDefaults(DefaultCamelContext context) throws Exception {
        var defaults = new Properties();
        try (InputStream in = InMemoryHoldRestartTest.class.getResourceAsStream("/application.properties")) {
            defaults.load(in);
        }
        var kafka = new HashMap<String, Object>();
        defaults.stringPropertyNames().stream()
                .filter(key -> key.startsWith("camel.component.kafka.") && !defaults.getProperty(key).contains("${"))
                .forEach(key -> kafka.put(StringHelper.dashToCamelCase(key.substring("camel.component.kafka.".length())),
                        defaults.getProperty(key)));
        var component = context.getComponent("kafka", KafkaComponent.class);
        PropertyBindingSupport.build().withCamelContext(context).withTarget(component)
                .withProperties(kafka).withMandatory(true).bind();
    }
}
