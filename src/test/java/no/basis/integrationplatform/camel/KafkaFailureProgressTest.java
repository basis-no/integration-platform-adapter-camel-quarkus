package no.basis.integrationplatform.camel;

import no.basis.integrationplatform.camel.lifecycle.MessageStoreRoutePolicyFactory;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

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
            admin.createTopics(List.of(new NewTopic(input, 1, (short) 1), new NewTopic(dlq, 1, (short) 1))).all().get(20, TimeUnit.SECONDS);
            var calls = new CopyOnWriteArrayList<String>();
            var outcomes = new CopyOnWriteArrayList<String>();
            var completed = new CountDownLatch(2);
            var journalCalls = new AtomicInteger();
            var dlqCalls = new AtomicInteger();
            var props = new Properties();
            props.setProperty("basis.message-store.dlq-uri", "direct:flaky-parking");
            context.getPropertiesComponent().setOverrideProperties(props);
            context.addRoutePolicyFactory(new MessageStoreRoutePolicyFactory(row -> {
                if (outage.equals("journal") && journalCalls.getAndIncrement() == 0) {
                    throw new IllegalStateException("synthetic temporary journal outage");
                }
                outcomes.add(row.status().name());
                if (!row.status().name().equals("FAILED")) completed.countDown();
            }, "order-pipeline", "postgres-inline", 100_000, 30, true));
            context.addRoutes(new PlatformRouteConfiguration());
            context.addRoutes(new PlatformDeadLetterRoute());
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("kafka:" + input + "?brokers=" + brokers + "&groupId=" + group
                            + "&autoOffsetReset=earliest&autoCommitEnable=false&allowManualCommit=true"
                            + "&breakOnFirstError=true&maxPollRecords=" + maxPollRecords + "&pollTimeoutMs=100")
                        .routeId("input").routeConfigurationId(PlatformRouteConfiguration.NO_RETRY)
                        .process(e -> {
                            String body = e.getMessage().getBody(String.class);
                            calls.add(body);
                            if (outage.equals("dlq") && body.equals("bad")) {
                                e.getMessage().setBody("transformed");
                                throw new IllegalArgumentException("synthetic receiver rejection");
                            }
                        });
                    from("direct:flaky-parking").errorHandler(noErrorHandler())
                        .process(e -> {
                            if (dlqCalls.getAndIncrement() == 0) throw new IllegalStateException("synthetic DLQ outage");
                            assertEquals("bad", e.getMessage().getBody(String.class));
                        })
                        .to("kafka:" + dlq + "?brokers=" + brokers);
                }
            });
            var producerProps = new HashMap<String, Object>(client);
            producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            try (var producer = new KafkaProducer<String, String>(producerProps)) {
                producer.send(new ProducerRecord<>(input, "key", "bad")).get(10, TimeUnit.SECONDS);
                producer.send(new ProducerRecord<>(input, "key", "good")).get(10, TimeUnit.SECONDS);
            }
            context.start();
            assertTrue(completed.await(30, TimeUnit.SECONDS), "Processing did not recover: " + calls + " / " + outcomes);
            assertEquals(List.of("bad", "bad", "good"), calls, "Do not move past unresolved input");
            // Completion callback precedes commit; wait boundedly for the broker-visible position.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            long offset = -1;
            while (System.nanoTime() < deadline && offset != 2) {
                var offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
                var position = offsets.get(new TopicPartition(input, 0));
                if (position != null) offset = position.offset();
                if (offset != 2) Thread.sleep(50);
            }
            assertEquals(2, offset);
            assertEquals("COMPLETED", outcomes.get(outcomes.size() - 1));
        }
    }
}
