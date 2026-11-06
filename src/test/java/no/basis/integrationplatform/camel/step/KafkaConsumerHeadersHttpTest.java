package no.basis.integrationplatform.camel.step;

import com.sun.net.httpserver.HttpServer;
import no.basis.integrationplatform.camel.PlatformDeadLetterRoute;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.PlatformRouteConfiguration;
import no.basis.integrationplatform.camel.lifecycle.MessageStoreRoutePolicyFactory;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A record consumed from a real broker and sent on to a raw HTTP receiver. The receiver gets
 * the route's headers and the record's own non-credential headers, but none of the Kafka
 * consumer's metadata headers: on Camel versions that name them {@code kafka.*}, such as
 * 4.18.0, HTTP producers sent them, and {@code kafka.HEADERS} listed every record header's
 * bytes, credentials included. The record's credential headers are withheld too, which
 * {@code vertx-http} sent as text; a credential header the route sets itself is sent.
 */
@Testcontainers
class KafkaConsumerHeadersHttpTest {
    private static final String CREDENTIAL = "Bearer synthetic-caller-credential";

    @Container
    static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @ParameterizedTest
    @ValueSource(strings = {"", "vertx-http:", "netty-http:"})
    void anHttpSendFromAKafkaRouteForwardsNoConsumerMetadataAndNoCredential(String component) throws Exception {
        String input = "input-" + UUID.randomUUID();
        String group = "group-" + UUID.randomUUID();
        String brokers = KAFKA.getBootstrapServers();
        var received = new CompletableFuture<Map<String, List<String>>>();
        var before = new CompletableFuture<Map<String, Object>>();
        var after = new CompletableFuture<Map<String, Object>>();
        HttpServer receiver = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        receiver.createContext("/", exchange -> {
            exchange.getRequestBody().readAllBytes();
            received.complete(new HashMap<>(exchange.getRequestHeaders()));
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        receiver.start();
        try (var admin = AdminClient.create(Map.<String, Object>of("bootstrap.servers", brokers));
             var context = new DefaultCamelContext()) {
            admin.createTopics(List.of(new NewTopic(input, 1, (short) 1))).all().get(20, TimeUnit.SECONDS);
            var outcomes = new CopyOnWriteArrayList<String>();
            var properties = new Properties();
            properties.setProperty("basis.message-store.dlq-uri", "direct:parking");
            context.getPropertiesComponent().setOverrideProperties(properties);
            context.getCamelContextExtension().addInterceptStrategy(
                    new StepTrackingInterceptStrategy(new DefaultStepTracker(new NoopStepOutcomeStore())));
            context.addRoutePolicyFactory(new MessageStoreRoutePolicyFactory(row -> outcomes.add(row.status().name()),
                    "order-pipeline", "postgres-inline", 100_000, 30, true, "dlq", "auto"));
            context.addRoutes(new PlatformRouteConfiguration());
            context.addRoutes(new PlatformDeadLetterRoute());
            String url = component + "http://127.0.0.1:" + receiver.getAddress().getPort() + "/orders";
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:parking").routeId("parking").errorHandler(noErrorHandler()).log("parked");
                    from("kafka:" + input + "?brokers=" + brokers + "&groupId=" + group
                            + "&autoOffsetReset=earliest&allowManualCommit=true&autoCommitEnable=false&pollTimeoutMs=100")
                            .routeId("input").routeConfigurationId(PlatformRouteConfiguration.NO_RETRY)
                            .setHeader("X-Route-Header", constant("route-value"))
                            .setHeader("X-Route-Token", constant("route-own"))
                            .process(e -> before.complete(Map.copyOf(e.getMessage().getHeaders())))
                            .to(url).id("post-order")
                            .process(e -> after.complete(Map.copyOf(e.getMessage().getHeaders())));
                }
            });
            var producerProperties = new HashMap<String, Object>(Map.of("bootstrap.servers", brokers));
            producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            try (var producer = new KafkaProducer<String, String>(producerProperties)) {
                var record = new ProducerRecord<>(input, "record-key", "{}");
                record.headers().add(new RecordHeader("Authorization", CREDENTIAL.getBytes(StandardCharsets.UTF_8)));
                record.headers().add(new RecordHeader(PlatformHeaders.MESSAGE_ID, "message-1".getBytes(StandardCharsets.UTF_8)));
                record.headers().add(new RecordHeader("X-Trace-Note", "trace".getBytes(StandardCharsets.UTF_8)));
                producer.send(record).get(10, TimeUnit.SECONDS);
            }
            context.start();

            Map<String, List<String>> headers = received.get(30, TimeUnit.SECONDS);
            var names = headers.keySet().stream().map(name -> name.toLowerCase(Locale.ROOT)).collect(Collectors.toSet());
            assertTrue(names.stream().noneMatch(name -> name.startsWith("kafka.") || name.startsWith("camelkafka")),
                    "No Kafka consumer metadata reaches the receiver: " + names);
            String values = headers.values().stream().flatMap(List::stream).collect(Collectors.joining("\n"));
            assertFalse(values.contains(byteList(CREDENTIAL)), "The credential is not sent as its byte list");
            assertFalse(values.contains(CREDENTIAL), "The credential is not sent as text");
            assertFalse(names.contains("authorization"), "The record's credential header is not sent");
            assertEquals(List.of("route-own"), headers.get("X-route-token"), "A credential the route sets is sent");

            assertEquals(List.of("route-value"), headers.get("X-route-header"), "The route's own headers are sent as before");
            assertEquals(List.of("message-1"), headers.get("X-bip-message-id"));
            assertNotNull(headers.get("X-trace-note"), "The record's other headers are sent as before");

            // The adapter restores what it withheld; Camel 4.18.3 and later name them CamelKafka*,
            // which the adapter leaves to Camel's filters.
            Map<String, Object> beforeSend = before.get(10, TimeUnit.SECONDS);
            Map<String, Object> afterSend = after.get(10, TimeUnit.SECONDS);
            for (String name : beforeSend.keySet()) {
                if (name.startsWith("kafka.")) {
                    assertEquals(beforeSend.get(name), afterSend.get(name), "The route still sees " + name + " after the send");
                }
            }
            assertTrue(afterSend.containsKey("Authorization"), "The route still sees the record's credential header");
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            long committed = -1;
            while ((outcomes.isEmpty() || committed != 1) && System.nanoTime() < deadline) {
                var position = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
                        .get(5, TimeUnit.SECONDS).get(new TopicPartition(input, 0));
                committed = position == null ? -1 : position.offset();
                Thread.sleep(50);
            }
            assertEquals(List.of("COMPLETED"), outcomes);
            assertEquals(1, committed, "The record is committed after its outcome, as before");
        } finally {
            receiver.stop(0);
        }
    }

    /**
     * The headers are back once the failed send completes, so the parked DLQ record keeps the
     * input's key, which Camel's Kafka producer takes from {@code kafka.KEY}, and the input is
     * committed after parking as before.
     */
    @ParameterizedTest
    @CsvSource({"'',to", "vertx-http:,to", "'',recipientList", "vertx-http:,recipientList", "'',routingSlip", "vertx-http:,routingSlip"})
    void aParkedRecordKeepsItsKeyAfterAFailedHttpSend(String component, String shape) throws Exception {
        String input = "input-" + UUID.randomUUID();
        String dlq = "dlq-" + UUID.randomUUID();
        String group = "group-" + UUID.randomUUID();
        String brokers = KAFKA.getBootstrapServers();
        var received = new CopyOnWriteArrayList<String>();
        HttpServer receiver = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        receiver.createContext("/", exchange -> {
            exchange.getRequestBody().readAllBytes();
            exchange.getRequestHeaders().keySet().forEach(name -> received.add(name.toLowerCase(Locale.ROOT)));
            exchange.sendResponseHeaders(500, -1);
            exchange.close();
        });
        receiver.start();
        try (var admin = AdminClient.create(Map.<String, Object>of("bootstrap.servers", brokers));
             var context = new DefaultCamelContext()) {
            admin.createTopics(List.of(new NewTopic(input, 1, (short) 1), new NewTopic(dlq, 1, (short) 1)))
                    .all().get(20, TimeUnit.SECONDS);
            var outcomes = new CopyOnWriteArrayList<String>();
            var properties = new Properties();
            properties.setProperty("basis.message-store.dlq-uri", "kafka:" + dlq + "?brokers=" + brokers);
            context.getPropertiesComponent().setOverrideProperties(properties);
            context.getCamelContextExtension().addInterceptStrategy(
                    new StepTrackingInterceptStrategy(new DefaultStepTracker(new NoopStepOutcomeStore())));
            context.addRoutePolicyFactory(new MessageStoreRoutePolicyFactory(row -> outcomes.add(row.status().name()),
                    "order-pipeline", "postgres-inline", 100_000, 30, true, "dlq", "auto"));
            context.addRoutes(new PlatformRouteConfiguration());
            context.addRoutes(new PlatformDeadLetterRoute());
            String url = component + "http://127.0.0.1:" + receiver.getAddress().getPort() + "/orders";
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    var route = from("kafka:" + input + "?brokers=" + brokers + "&groupId=" + group
                            + "&autoOffsetReset=earliest&allowManualCommit=true&autoCommitEnable=false&pollTimeoutMs=100")
                            .routeId("input").routeConfigurationId(PlatformRouteConfiguration.NO_RETRY);
                    switch (shape) {
                        case "recipientList" -> route.recipientList(constant(url));
                        case "routingSlip" -> route.routingSlip(constant(url));
                        default -> route.to(url).id("post-order");
                    }
                }
            });
            var producerProperties = new HashMap<String, Object>(Map.of("bootstrap.servers", brokers));
            producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            try (var producer = new KafkaProducer<String, String>(producerProperties)) {
                var record = new ProducerRecord<>(input, "record-key", "{}");
                record.headers().add(new RecordHeader("Authorization", CREDENTIAL.getBytes(StandardCharsets.UTF_8)));
                record.headers().add(new RecordHeader(PlatformHeaders.MESSAGE_ID, "message-1".getBytes(StandardCharsets.UTF_8)));
                producer.send(record).get(10, TimeUnit.SECONDS);
            }
            context.start();

            var consumerProperties = new HashMap<String, Object>(Map.of("bootstrap.servers", brokers,
                    "group.id", "check-" + UUID.randomUUID(), "auto.offset.reset", "earliest"));
            consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            var parked = new java.util.ArrayList<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>>();
            try (var consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(consumerProperties)) {
                consumer.subscribe(List.of(dlq));
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (parked.isEmpty() && System.nanoTime() < deadline) {
                    consumer.poll(java.time.Duration.ofMillis(200)).forEach(parked::add);
                }
            }
            assertEquals(1, parked.size(), "The input is parked once");
            assertEquals("record-key", parked.getFirst().key(), "The parked record keeps the input's key");
            assertEquals("message-1", new String(parked.getFirst().headers().lastHeader(PlatformHeaders.MESSAGE_ID).value(),
                    StandardCharsets.UTF_8));
            assertTrue(received.stream().noneMatch(name -> name.startsWith("kafka.") || name.equals("authorization")),
                    "The receiver got " + received);

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            long committed = -1;
            while ((outcomes.isEmpty() || committed != 1) && System.nanoTime() < deadline) {
                var position = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
                        .get(5, TimeUnit.SECONDS).get(new TopicPartition(input, 0));
                committed = position == null ? -1 : position.offset();
                Thread.sleep(50);
            }
            assertEquals(List.of("DLQ"), outcomes);
            assertEquals(1, committed, "The input is committed after parking, as before");
        } finally {
            receiver.stop(0);
        }
    }

    /** How Camel's http component writes a byte array header: its list of numbers, as in {@code [66, 101]}. */
    private static String byteList(String value) {
        return Arrays.toString(value.getBytes(StandardCharsets.UTF_8)).replace("[", "").replace("]", "");
    }
}
