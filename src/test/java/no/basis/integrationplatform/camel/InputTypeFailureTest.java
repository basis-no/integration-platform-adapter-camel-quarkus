package no.basis.integrationplatform.camel;

import no.basis.integrationplatform.camel.lifecycle.MessageStoreRoutePolicyFactory;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.component.kafka.KafkaComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.PropertyBindingSupport;
import org.apache.camel.util.StringHelper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The failure mode follows the input type, in one application with the default
 * {@code basis.message-store.failure-mode=dlq}: a Kafka boundary parks a message that exhausted
 * its retries and commits; a file input and a JMS queue input, normalized to Kafka, hand the
 * failure back to their source, which keeps the file and redelivers the queue message, and
 * nothing of theirs is parked. HTTP is covered by {@link MixedBoundaryFailureModeTest}.
 */
@Testcontainers
class InputTypeFailureTest {
    @Container
    static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @Container
    static final GenericContainer<?> BROKER = new GenericContainer<>(DockerImageName.parse("apache/artemis:2.57.0-alpine"))
            .withEnv("ARTEMIS_USER", "artemis")
            .withEnv("ARTEMIS_PASSWORD", "artemis")
            .withExposedPorts(61616)
            .waitingFor(Wait.forLogMessage(".*Server is now active.*", 1).withStartupTimeout(Duration.ofMinutes(2)));

    @TempDir
    Path inbox;

    @Test
    void eachInputTypeHandsItsFailureToItsOwnRecoveryPath() throws Exception {
        String topic = "bip.orders-" + UUID.randomUUID() + ".in";
        String queue = "orders-" + UUID.randomUUID();
        String brokers = KAFKA.getBootstrapServers();
        var rows = new CopyOnWriteArrayList<MessageAttemptResult>();
        var parked = new CopyOnWriteArrayList<String>();
        // Each normalization route fails until its source has delivered the item twice:
        // four tries of the first delivery (one plus three redeliveries), then the source's own
        // second delivery succeeds.
        var tries = new ConcurrentHashMap<String, AtomicInteger>();
        var factory = new MessageStoreRoutePolicyFactory(
                rows::add, "orders", "postgres-inline", 1_048_576, 30, true, "dlq", "orders-input");

        try (var admin = AdminClient.create(Map.of("bootstrap.servers", brokers)); var context = new DefaultCamelContext()) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(20, TimeUnit.SECONDS);
            bindShippedKafkaDefaults(context);
            var properties = new Properties();
            properties.setProperty("basis.message-store.dlq-uri", "direct:parking");
            context.getPropertiesComponent().setOverrideProperties(properties);
            String brokerUrl = "tcp://" + BROKER.getHost() + ":" + BROKER.getMappedPort(61616);
            context.addComponent("jms", JmsComponent.jmsComponentClientAcknowledge(
                    new ActiveMQConnectionFactory(brokerUrl, "artemis", "artemis")));
            context.addRoutePolicyFactory(factory);
            context.addRoutes(new PlatformRouteConfiguration());
            context.addRoutes(new PlatformDeadLetterRoute());
            context.addRoutes(new RouteBuilder() {
                @Override
                public void configure() {
                    from("kafka:" + topic + "?brokers=" + brokers + "&groupId=group-" + UUID.randomUUID()
                            + "&autoOffsetReset=earliest&pollTimeoutMs=100")
                            .routeId("orders-input").routeConfigurationId(PlatformRouteConfiguration.NO_RETRY)
                            .process(e -> {
                                if ("kafka-order".equals(e.getMessage().getBody(String.class))) {
                                    throw new IllegalStateException("carrier rejected");
                                }
                            });
                    from("file:" + inbox + "?move=.done/${file:name}&delay=200&initialDelay=0")
                            .routeId("file-to-kafka").routeConfigurationId(PlatformRouteConfiguration.INGRESS_NORMALIZATION)
                            .convertBodyTo(String.class)
                            .process(e -> failFirstDelivery(tries, "file"))
                            .to("kafka:" + topic + "?brokers=" + brokers);
                    from("jms:queue:" + queue)
                            .routeId("queue-to-kafka").routeConfigurationId(PlatformRouteConfiguration.INGRESS_NORMALIZATION)
                            .convertBodyTo(String.class)
                            .process(e -> failFirstDelivery(tries, "jms"))
                            .to("kafka:" + topic + "?brokers=" + brokers);
                    from("direct:parking").routeId("parking").errorHandler(noErrorHandler())
                            .process(e -> parked.add(e.getMessage().getBody(String.class)));
                }
            });
            context.start();

            Files.writeString(inbox.resolve("order.xml"), "file-order");
            try (var template = context.createProducerTemplate()) {
                template.sendBody("jms:queue:" + queue, "jms-order");
            }
            var producerProperties = new HashMap<String, Object>(Map.of("bootstrap.servers", brokers));
            producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            try (var producer = new KafkaProducer<String, String>(producerProperties)) {
                producer.send(new ProducerRecord<>(topic, "key", "kafka-order")).get(10, TimeUnit.SECONDS);
            }

            // File: the first delivery exhausted its tries and the file stayed where it was.
            await(() -> tries.containsKey("file") && tries.get("file").get() >= 4, "the file's first delivery");
            // Both normalized inputs arrive in the end, through their source's own second delivery.
            await(() -> completed(rows, "file-order") && completed(rows, "jms-order"),
                    () -> "both normalized inputs; tries " + tries + ", rows " + rows.stream()
                            .map(r -> r.status() + ":" + payload(r)).toList() + ", parked " + parked);
            await(() -> Files.exists(inbox.resolve(".done/order.xml")), "the file to be moved once published");
            await(() -> parked.contains("kafka-order"), "the Kafka input to be parked");
        }

        assertEquals(5, tries.get("file").get(), "four tries of the first poll, then the second poll");
        assertEquals(5, tries.get("jms").get(), "four tries, then the broker's redelivery");
        assertEquals(List.of("kafka-order"), parked, "Only the Kafka input is parked; the sources keep theirs");
    }

    // A fifth try can only come from the source delivering the item again: for the file, the next
    // poll reading the same, unmoved file; for the queue, the broker's redelivery.
    private static void failFirstDelivery(Map<String, AtomicInteger> tries, String source) {
        if (tries.computeIfAbsent(source, s -> new AtomicInteger()).incrementAndGet() <= 4) {
            throw new IllegalStateException(source + " publish unavailable");
        }
    }

    private static boolean completed(List<MessageAttemptResult> rows, String body) {
        return rows.stream().anyMatch(r -> r.status() == MessageStatus.COMPLETED && body.equals(payload(r)));
    }

    private static String payload(MessageAttemptResult row) {
        return row.payloadBytes() == null ? null : new String(row.payloadBytes(), java.nio.charset.StandardCharsets.UTF_8);
    }

    private static void await(BooleanSupplier condition, String what) throws InterruptedException {
        await(condition, () -> what);
    }

    private static void await(BooleanSupplier condition, java.util.function.Supplier<String> what) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                fail("Timed out waiting for " + what.get());
            }
            Thread.sleep(100);
        }
    }

    private static void bindShippedKafkaDefaults(DefaultCamelContext context) throws Exception {
        var defaults = new Properties();
        try (InputStream in = InputTypeFailureTest.class.getResourceAsStream("/application.properties")) {
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
