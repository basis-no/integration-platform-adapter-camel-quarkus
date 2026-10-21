package no.basis.integrationplatform.camel;

import no.basis.integrationplatform.camel.lifecycle.MessageStoreRoutePolicyFactory;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.camel.component.kafka.KafkaComponent;
import org.apache.camel.support.PropertyBindingSupport;
import org.apache.camel.util.StringHelper;
import org.junit.jupiter.api.Test;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
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
    static final ConfluentKafkaContainer KAFKA = boundedKafka();

    private static ConfluentKafkaContainer boundedKafka() {
        boolean bounded = Boolean.getBoolean("bip.test.bounded-services");
        String runId = System.getProperty("bip.test.run-id");
        if (bounded && (runId == null || runId.isBlank())) {
            throw new IllegalArgumentException("Unique bip.test.run-id is required");
        }
        var kafka = new ConfluentKafkaContainer(DockerImageName.parse(
                bounded ? "confluentinc/cp-kafka@sha256:620734d9fc0bb1f9886932e5baf33806074469f40e3fe246a3fdbb59309535fa" : "confluentinc/cp-kafka:7.6.1"));
        if (bounded) {
            kafka.withImagePullPolicy(image -> false);
            kafka.withLabel("bip.qualification.run", runId);
            kafka.withEnv("KAFKA_HEAP_OPTS", "-Xms128m -Xmx256m");
            kafka.withCreateContainerCmdModifier(command -> {
                var ports = command.getExposedPorts();
                if (ports == null || ports.length == 0) throw new IllegalStateException("Missing synthetic fixture ports");
                var bindings = java.util.Arrays.stream(ports)
                        .map(port -> new com.github.dockerjava.api.model.PortBinding(
                                com.github.dockerjava.api.model.Ports.Binding.bindIp("127.0.0.1"), port))
                        .toArray(com.github.dockerjava.api.model.PortBinding[]::new);
                command.getHostConfig().withMemory(768L * 1024 * 1024).withMemorySwap(768L * 1024 * 1024)
                        .withPidsLimit(256L).withNanoCPUs(1_000_000_000L).withPortBindings(bindings);
            });
        }
        return kafka;
    }

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
            // Exercise durability failures with the consumer defaults shipped to applications.
            var defaults = new Properties();
            try (InputStream in = getClass().getResourceAsStream("/application.properties")) {
                defaults.load(in);
            }
            var kafka = new HashMap<String, Object>();
            defaults.stringPropertyNames().stream()
                .filter(key -> key.startsWith("camel.component.kafka.") && !defaults.getProperty(key).contains("${"))
                .forEach(key -> kafka.put(StringHelper.dashToCamelCase(key.substring("camel.component.kafka.".length())), defaults.getProperty(key)));
            PropertyBindingSupport.build().withCamelContext(context).withTarget(context.getComponent("kafka", KafkaComponent.class))
                .withProperties(kafka).withMandatory(true).bind();
            assertEquals(org.apache.camel.component.kafka.PollOnError.RECONNECT,
                    context.getComponent("kafka", KafkaComponent.class).getConfiguration().getPollOnError());
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

    /** Fails the first deserialization, as a poll-time failure that clears on its own. */
    public static class TransientPollFailure implements Deserializer<String> {
        static final AtomicInteger FAILURES = new AtomicInteger();
        @Override
        public String deserialize(String topic, byte[] data) {
            if (FAILURES.getAndDecrement() > 0) throw new SerializationException("synthetic transient poll failure");
            return data == null ? null : new String(data, StandardCharsets.UTF_8);
        }
    }

    @Test
    void transientPollFailureReconnectsWithoutSkippingOrJournaling() throws Exception {
        String input = "input-" + UUID.randomUUID();
        String brokers = KAFKA.getBootstrapServers();
        var client = Map.<String, Object>of("bootstrap.servers", brokers);
        try (var admin = AdminClient.create(client); var context = new DefaultCamelContext()) {
            admin.createTopics(List.of(new NewTopic(input, 1, (short) 1))).all().get(20, TimeUnit.SECONDS);
            // The adapter's packaged Kafka consumer defaults, as the runtime applies them.
            var defaults = new Properties();
            try (InputStream in = getClass().getResourceAsStream("/application.properties")) {
                defaults.load(in);
            }
            var kafka = new HashMap<String, Object>();
            defaults.stringPropertyNames().stream()
                .filter(key -> key.startsWith("camel.component.kafka.") && !defaults.getProperty(key).contains("${"))
                .forEach(key -> kafka.put(StringHelper.dashToCamelCase(key.substring("camel.component.kafka.".length())), defaults.getProperty(key)));
            PropertyBindingSupport.build().withCamelContext(context).withTarget(context.getComponent("kafka", KafkaComponent.class))
                .withProperties(kafka).withMandatory(true).bind();
            var props = new Properties();
            props.setProperty("basis.message-store.dlq-uri", "direct:parking");
            context.getPropertiesComponent().setOverrideProperties(props);
            var calls = new CopyOnWriteArrayList<String>();
            var outcomes = new CopyOnWriteArrayList<String>();
            var completed = new CountDownLatch(1);
            context.addRoutePolicyFactory(new MessageStoreRoutePolicyFactory(row -> {
                outcomes.add(row.status().name());
                if (row.status().name().equals("COMPLETED")) completed.countDown();
            }, "order-pipeline", "postgres-inline", 100_000, 30, true));
            context.addRoutes(new PlatformRouteConfiguration());
            context.addRoutes(new PlatformDeadLetterRoute());
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("kafka:" + input + "?brokers=" + brokers + "&groupId=group-" + UUID.randomUUID()
                            + "&autoOffsetReset=earliest&pollTimeoutMs=100&valueDeserializer=" + TransientPollFailure.class.getName())
                        .routeId("input").routeConfigurationId(PlatformRouteConfiguration.NO_RETRY)
                        .process(e -> calls.add(e.getMessage().getBody(String.class)));
                    from("direct:parking").errorHandler(noErrorHandler()).log("parked");
                }
            });
            var producerProps = new HashMap<String, Object>(client);
            producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            try (var producer = new KafkaProducer<String, String>(producerProps)) {
                producer.send(new ProducerRecord<>(input, "key", "only")).get(10, TimeUnit.SECONDS);
            }
            TransientPollFailure.FAILURES.set(1);
            context.start();
            assertTrue(completed.await(30, TimeUnit.SECONDS), "The consumer did not recover from a poll failure: " + calls + " / " + outcomes);
            assertEquals(List.of("only"), calls, "A poll failure must not skip the record");
            assertEquals(List.of("COMPLETED"), outcomes, "A poll failure is not a message and must not be journaled");
        }
    }
}
