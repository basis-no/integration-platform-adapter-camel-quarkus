package no.basis.integrationplatform.camel;

import no.basis.integrationplatform.camel.lifecycle.MessageStoreRoutePolicyFactory;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.PropertyBindingSupport;
import org.apache.camel.util.StringHelper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A record whose outcome is not durable, because the journal or the DLQ publish failed, is
 * not committed and is read again before the next record, with the consumer defaults the
 * adapter ships.
 */
@Testcontainers
class KafkaFailureProgressTest {
    @Container
    static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @ParameterizedTest
    @CsvSource({"journal,1", "journal,10", "dlq,1", "dlq,10"})
    void transientDurabilityFailureRetriesBlockedInputBeforeFollowingRecord(String outage, int maxPollRecords) throws Exception {
        String input = "input-" + UUID.randomUUID();
        String dlq = "dlq-" + UUID.randomUUID();
        String group = "group-" + UUID.randomUUID();
        String brokers = KAFKA.getBootstrapServers();
        var client = Map.<String, Object>of("bootstrap.servers", brokers);
        try (var admin = AdminClient.create(client); var context = new DefaultCamelContext()) {
            admin.createTopics(List.of(new NewTopic(input, 1, (short) 1), new NewTopic(dlq, 1, (short) 1)))
                    .all().get(20, TimeUnit.SECONDS);
            var calls = new CopyOnWriteArrayList<String>();
            var outcomes = new CopyOnWriteArrayList<String>();
            var completed = new CountDownLatch(2);
            var journalCalls = new AtomicInteger();
            var dlqCalls = new AtomicInteger();
            bindShippedKafkaDefaults(context);
            var properties = new Properties();
            properties.setProperty("basis.message-store.dlq-uri", "direct:flaky-parking");
            context.getPropertiesComponent().setOverrideProperties(properties);
            context.addRoutePolicyFactory(new MessageStoreRoutePolicyFactory(row -> {
                if (outage.equals("journal") && journalCalls.getAndIncrement() == 0) {
                    throw new IllegalStateException("synthetic temporary journal outage");
                }
                outcomes.add(row.status().name());
                if (!row.status().name().equals("FAILED")) completed.countDown();
            }, "order-pipeline", "postgres-inline", 100_000, 30, true, "dlq", "auto"));
            context.addRoutes(new PlatformRouteConfiguration());
            context.addRoutes(new PlatformDeadLetterRoute());
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("kafka:" + input + "?brokers=" + brokers + "&groupId=" + group
                            + "&autoOffsetReset=earliest&maxPollRecords=" + maxPollRecords + "&pollTimeoutMs=100")
                            .routeId("input").routeConfigurationId(PlatformRouteConfiguration.NO_RETRY)
                            .process(e -> {
                                String body = e.getMessage().getBody(String.class);
                                calls.add(body);
                                if (outage.equals("dlq") && body.equals("bad")) {
                                    e.getMessage().setBody("transformed");
                                    throw new IllegalArgumentException("synthetic receiver rejection");
                                }
                            });
                    from("direct:flaky-parking").routeId("flaky-parking").errorHandler(noErrorHandler())
                            .process(e -> {
                                if (dlqCalls.getAndIncrement() == 0) throw new IllegalStateException("synthetic DLQ outage");
                                assertEquals("bad", e.getMessage().getBody(String.class));
                            })
                            .to("kafka:" + dlq + "?brokers=" + brokers).id("publish-dlq");
                }
            });
            var producerProperties = new HashMap<String, Object>(client);
            producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            try (var producer = new KafkaProducer<String, String>(producerProperties)) {
                producer.send(new ProducerRecord<>(input, "key", "bad")).get(10, TimeUnit.SECONDS);
                producer.send(new ProducerRecord<>(input, "key", "good")).get(10, TimeUnit.SECONDS);
            }
            context.start();
            assertTrue(completed.await(30, TimeUnit.SECONDS), "Processing did not recover: " + calls + " / " + outcomes);
            assertEquals(List.of("bad", "bad", "good"), calls, "Do not move past unresolved input");
            assertEquals(outage.equals("dlq") ? List.of("FAILED", "DLQ", "COMPLETED") : List.of("COMPLETED", "COMPLETED"),
                    outcomes);
            // The completion callback runs before the commit; wait for the broker-visible position.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            long offset = -1;
            while (System.nanoTime() < deadline && offset != 2) {
                var offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
                var position = offsets.get(new TopicPartition(input, 0));
                if (position != null) offset = position.offset();
                if (offset != 2) Thread.sleep(50);
            }
            assertEquals(2, offset);
        }
    }

    @Test
    void aCollectionBodyIsParkedAsOneRecord() throws Exception {
        // Without a captured payload the body is parked as it is; here an empty list, which a
        // Kafka producer with useIterator=true would send as no record at all.
        String input = "input-" + UUID.randomUUID();
        String dlq = "dlq-" + UUID.randomUUID();
        String group = "group-" + UUID.randomUUID();
        String brokers = KAFKA.getBootstrapServers();
        var client = Map.<String, Object>of("bootstrap.servers", brokers);
        try (var admin = AdminClient.create(client); var context = new DefaultCamelContext()) {
            admin.createTopics(List.of(new NewTopic(input, 1, (short) 1), new NewTopic(dlq, 1, (short) 1)))
                    .all().get(20, TimeUnit.SECONDS);
            var outcomes = new CopyOnWriteArrayList<String>();
            var recorded = new CountDownLatch(1);
            bindShippedKafkaDefaults(context);
            var properties = new Properties();
            properties.setProperty("basis.message-store.dlq-uri", "kafka:" + dlq + "?brokers=" + brokers);
            context.getPropertiesComponent().setOverrideProperties(properties);
            context.addRoutePolicyFactory(new MessageStoreRoutePolicyFactory(row -> {
                outcomes.add(row.status().name());
                recorded.countDown();
            }, "order-pipeline", "metadata-only", 100_000, 30, false, "dlq", "auto"));
            context.addRoutes(new PlatformRouteConfiguration());
            context.addRoutes(new PlatformDeadLetterRoute());
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("kafka:" + input + "?brokers=" + brokers + "&groupId=" + group
                            + "&autoOffsetReset=earliest&pollTimeoutMs=100")
                            .routeId("input").routeConfigurationId(PlatformRouteConfiguration.NO_RETRY)
                            .process(e -> {
                                e.getMessage().setBody(List.of());
                                throw new IllegalArgumentException("synthetic receiver rejection");
                            });
                }
            });
            var producerProperties = new HashMap<String, Object>(client);
            producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            try (var producer = new KafkaProducer<String, String>(producerProperties)) {
                producer.send(new ProducerRecord<>(input, "key", "bad")).get(10, TimeUnit.SECONDS);
            }
            context.start();
            assertTrue(recorded.await(30, TimeUnit.SECONDS), "No outcome recorded");
            assertEquals(List.of("DLQ"), outcomes);
            var dlqPartition = new TopicPartition(dlq, 0);
            long dlqEnd = admin.listOffsets(Map.of(dlqPartition, OffsetSpec.latest())).partitionResult(dlqPartition)
                    .get(10, TimeUnit.SECONDS).offset();
            assertEquals(1, dlqEnd, "The parked message is one DLQ record");
        }
    }

    @Test
    void aStoppedExchangeThatFailsIsNotCommitted() throws Exception {
        // Camel skips the adapter's parking for an exchange that its route stopped. The record
        // is recorded FAILED, not committed, and read again.
        String input = "input-" + UUID.randomUUID();
        String dlq = "dlq-" + UUID.randomUUID();
        String group = "group-" + UUID.randomUUID();
        String brokers = KAFKA.getBootstrapServers();
        var client = Map.<String, Object>of("bootstrap.servers", brokers);
        try (var admin = AdminClient.create(client); var context = new DefaultCamelContext()) {
            admin.createTopics(List.of(new NewTopic(input, 1, (short) 1), new NewTopic(dlq, 1, (short) 1)))
                    .all().get(20, TimeUnit.SECONDS);
            var outcomes = new CopyOnWriteArrayList<String>();
            var recorded = new CountDownLatch(1);
            bindShippedKafkaDefaults(context);
            var properties = new Properties();
            properties.setProperty("basis.message-store.dlq-uri", "kafka:" + dlq + "?brokers=" + brokers);
            context.getPropertiesComponent().setOverrideProperties(properties);
            context.addRoutePolicyFactory(new MessageStoreRoutePolicyFactory(row -> {
                outcomes.add(row.status().name());
                recorded.countDown();
            }, "order-pipeline", "postgres-inline", 100_000, 30, true, "dlq", "auto"));
            context.addRoutes(new PlatformRouteConfiguration());
            context.addRoutes(new PlatformDeadLetterRoute());
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("kafka:" + input + "?brokers=" + brokers + "&groupId=" + group
                            + "&autoOffsetReset=earliest&pollTimeoutMs=100")
                            .routeId("input").routeConfigurationId(PlatformRouteConfiguration.NO_RETRY)
                            .process(e -> {
                                e.setRouteStop(true);
                                throw new IllegalArgumentException("synthetic receiver rejection");
                            });
                }
            });
            var producerProperties = new HashMap<String, Object>(client);
            producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            try (var producer = new KafkaProducer<String, String>(producerProperties)) {
                producer.send(new ProducerRecord<>(input, "key", "bad")).get(10, TimeUnit.SECONDS);
            }
            context.start();
            assertTrue(recorded.await(30, TimeUnit.SECONDS), "No outcome recorded");
            // Read again: at least a second attempt before checking that nothing was committed.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (System.nanoTime() < deadline && outcomes.size() < 2) Thread.sleep(50);
            context.stop();
            assertTrue(outcomes.size() >= 2, "The record was read again: " + outcomes);
            assertTrue(outcomes.stream().allMatch("FAILED"::equals), outcomes.toString());
            var dlqPartition = new TopicPartition(dlq, 0);
            long dlqEnd = admin.listOffsets(Map.of(dlqPartition, OffsetSpec.latest())).partitionResult(dlqPartition)
                    .get(10, TimeUnit.SECONDS).offset();
            assertEquals(0, dlqEnd);
            assertNull(admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
                    .get(5, TimeUnit.SECONDS).get(new TopicPartition(input, 0)), "No offset was committed");
        }
    }

    /** Binds the Kafka component defaults from the adapter's application.properties, as the runtime does. */
    private static void bindShippedKafkaDefaults(DefaultCamelContext context) throws Exception {
        var defaults = new Properties();
        try (InputStream in = KafkaFailureProgressTest.class.getResourceAsStream("/application.properties")) {
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
