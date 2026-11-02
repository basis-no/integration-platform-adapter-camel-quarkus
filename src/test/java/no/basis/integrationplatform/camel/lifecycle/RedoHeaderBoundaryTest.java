package no.basis.integrationplatform.camel.lifecycle;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpServer;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.model.StepStatus;
import no.basis.integrationplatform.camel.step.DefaultStepClassifier;
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
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * A replay lists the steps to send again in x-bip-redo-steps. The list belongs to the
 * replayed attempt of this integration: a receiver or a downstream integration that got it
 * would redo its own steps with the same id for the same message id.
 */
@Testcontainers
class RedoHeaderBoundaryTest {
    @Container
    static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));
    private static final String REDO = "[\"post-receiver\",\"publish-downstream\"]";

    @ParameterizedTest(name = "managed Kafka input {0}")
    @ValueSource(booleans = {true, false})
    void aReplayRedoesItsStepsWithoutSendingTheListOnHttpOrKafka(boolean managedKafkaInput) throws Exception {
        String topic = "out-" + UUID.randomUUID();
        String brokers = KAFKA.getBootstrapServers();
        List<Headers> requests = new CopyOnWriteArrayList<>();
        List<MessageAttemptResult> rows = new CopyOnWriteArrayList<>();
        // Both steps were delivered by the original attempt.
        Set<String> ledger = Collections.synchronizedSet(new HashSet<>(Set.of(
                "message-1:post-receiver", "message-1:publish-downstream")));
        StepOutcomeStore store = new StepOutcomeStore() {
            public boolean wasDelivered(String key) { return ledger.contains(key); }
            public void recordDelivered(StepResult step) { ledger.add(step.idempotencyKey()); }
            public void recordFailed(StepResult step) { }
        };
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", x -> {
            requests.add(x.getRequestHeaders());
            x.getRequestBody().readAllBytes();
            x.sendResponseHeaders(200, -1);
            x.close();
        });
        server.start();
        try (var admin = AdminClient.create(Map.<String, Object>of("bootstrap.servers", brokers));
             var context = new DefaultCamelContext()) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(20, TimeUnit.SECONDS);
            // An HTTP POST is a side effect only when configured as one.
            var classifier = new DefaultStepClassifier(Optional.empty(), Optional.empty(),
                    Optional.of("post-receiver"), Optional.empty());
            context.getCamelContextExtension().addInterceptStrategy(
                    new StepTrackingInterceptStrategy(new DefaultStepTracker(store, classifier)));
            String receiver = "http://127.0.0.1:" + server.getAddress().getPort() + "/orders";
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    var route = from("direct:input").routeId("orders-input");
                    if (managedKafkaInput) {
                        route.routePolicy(new MessageStoreRoutePolicy(rows::add, "orders"));
                    }
                    route.to(receiver).id("post-receiver")
                            .to("kafka:" + topic + "?brokers=" + brokers).id("publish-downstream");
                }
            });
            context.start();
            context.createProducerTemplate().send("direct:input", e -> {
                if (managedKafkaInput) {
                    e.getMessage().setHeader(KafkaConstants.TOPIC, "orders.in");
                    e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, (KafkaManualCommit) () -> { });
                }
                // A Kafka record's headers are bytes; an HTTP intake's are text.
                e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, value(managedKafkaInput, "message-1"));
                e.getMessage().setHeader(PlatformHeaders.ATTEMPT, value(managedKafkaInput, "2"));
                e.getMessage().setHeader(PlatformHeaders.REPLAY, value(managedKafkaInput, "true"));
                e.getMessage().setHeader(PlatformHeaders.REDO_STEPS, value(managedKafkaInput, REDO));
                e.getMessage().setBody("<Order/>");
            });
        } finally {
            server.stop(0);
        }

        assertEquals(1, requests.size(), "The listed HTTP step was sent again");
        var record = consumeOne(brokers, topic);
        assertAll(
                () -> assertNull(requests.getFirst().getFirst(PlatformHeaders.REDO_STEPS), "No redo list on the HTTP request"),
                () -> assertNull(header(record, PlatformHeaders.REDO_STEPS), "No redo list on the published record"),
                // x-bip-replay is still forwarded: nothing in the adapter acts on it at a Kafka input.
                () -> assertEquals("true", requests.getFirst().getFirst(PlatformHeaders.REPLAY)),
                () -> assertEquals("true", header(record, PlatformHeaders.REPLAY)));
        if (managedKafkaInput) {
            assertEquals(1, rows.size());
            assertEquals(List.of(StepStatus.DELIVERED, StepStatus.DELIVERED),
                    rows.getFirst().steps().stream().map(StepResult::status).toList());
        }
    }

    private static Object value(boolean bytes, String value) {
        return bytes ? value.getBytes(StandardCharsets.UTF_8) : value;
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
