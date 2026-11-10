package no.basis.integrationplatform.camel;

import no.basis.integrationplatform.camel.lifecycle.MessageStoreRoutePolicyFactory;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.PropertyBindingSupport;
import org.apache.camel.util.StringHelper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A record that is not committed is read again after the consumer reconnects, also on a
 * partition where the consumer group has no committed offset yet. The reconnect after a
 * failed record resumes from the committed offset, and without one from the consumer's
 * {@code auto.offset.reset}: with {@code latest} it would resume at the end of the partition
 * and skip the record.
 */
@Testcontainers
class KafkaOffsetResetTest {
    @Container
    static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @Test
    void aFailedFirstRecordOnAPartitionWithoutACommittedOffsetIsReadAgain() throws Exception {
        String input = "input-" + UUID.randomUUID();
        String group = "group-" + UUID.randomUUID();
        String brokers = KAFKA.getBootstrapServers();
        var client = Map.<String, Object>of("bootstrap.servers", brokers);
        try (var admin = AdminClient.create(client); var context = new DefaultCamelContext()) {
            admin.createTopics(List.of(new NewTopic(input, 1, (short) 1))).all().get(20, TimeUnit.SECONDS);
            var calls = new CopyOnWriteArrayList<String>();
            var firstCall = new CountDownLatch(1);
            var completed = new CountDownLatch(1);
            var journalCalls = new AtomicInteger();
            bindShippedKafkaDefaults(context);
            context.addRoutePolicyFactory(new MessageStoreRoutePolicyFactory(row -> {
                // The first outcome cannot be made durable, so the first record is not committed.
                if (journalCalls.getAndIncrement() == 0) {
                    throw new IllegalStateException("synthetic temporary journal outage");
                }
                if (row.status().name().equals("COMPLETED") && "good".equals(calls.get(calls.size() - 1))) {
                    completed.countDown();
                }
            }, "order-pipeline", "postgres-inline", 100_000, 30, true));
            context.addRoutes(new PlatformRouteConfiguration());
            context.addRoutes(new PlatformDeadLetterRoute());
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    // No autoOffsetReset on the endpoint: the shipped consumer defaults apply.
                    from("kafka:" + input + "?brokers=" + brokers + "&groupId=" + group + "&pollTimeoutMs=100")
                            .routeId("input")
                            .process(e -> {
                                calls.add(e.getMessage().getBody(String.class));
                                firstCall.countDown();
                            });
                }
            });
            context.start();
            awaitStableGroup(admin, group);

            var producerProperties = new HashMap<String, Object>(client);
            producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            try (var producer = new KafkaProducer<String, String>(producerProperties)) {
                producer.send(new ProducerRecord<>(input, "key", "bad")).get(10, TimeUnit.SECONDS);
                assertTrue(firstCall.await(30, TimeUnit.SECONDS), "The first record was not consumed");
                // Give the consumer time to reconnect after the uncommitted record, then send the next one.
                Thread.sleep(3000);
                producer.send(new ProducerRecord<>(input, "key", "good")).get(10, TimeUnit.SECONDS);
            }
            assertTrue(completed.await(30, TimeUnit.SECONDS), "The later record was not processed: " + calls);
            assertEquals(List.of("bad", "bad", "good"), calls,
                    "The uncommitted first record must be read again, not skipped after the reconnect");
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

    private static void awaitStableGroup(AdminClient admin, String group) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            var description = admin.describeConsumerGroups(List.of(group)).all().get(5, TimeUnit.SECONDS).get(group);
            if (description.state() == ConsumerGroupState.STABLE
                    && description.members().stream().anyMatch(member -> !member.assignment().topicPartitions().isEmpty())) {
                // The assigned consumer resolves its position on its next poll.
                Thread.sleep(1000);
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("The consumer group did not get an assignment");
    }

    private static void bindShippedKafkaDefaults(DefaultCamelContext context) throws Exception {
        var defaults = new Properties();
        try (InputStream in = KafkaOffsetResetTest.class.getResourceAsStream("/application.properties")) {
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
