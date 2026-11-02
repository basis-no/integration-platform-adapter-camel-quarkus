package no.basis.integrationplatform.camel.step;

import no.basis.integrationplatform.camel.LogCapture;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.PublishedCredentialHeaders;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaHeaderFilterStrategy;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An HTTP intake that keeps the caller's headers publishes the caller's Authorization with the
 * message, readable by anyone who can read the topic. The adapter warns and publishes as before.
 */
@Testcontainers
class PublishedCredentialHeadersTest {
    @Container
    static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @Test
    void warnsOncePerRouteAndHeaderNameAndPublishesAsBefore() throws Exception {
        String topic = "intake-" + UUID.randomUUID();
        String brokers = KAFKA.getBootstrapServers();
        String kafka = "kafka:" + topic + "?brokers=" + brokers;
        try (var logs = LogCapture.of(PublishedCredentialHeaders.class);
             var admin = AdminClient.create(Map.<String, Object>of("bootstrap.servers", brokers));
             var context = new DefaultCamelContext()) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(20, TimeUnit.SECONDS);
            context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(new NoStepTracker()));
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:intake").routeId("orders-intake")
                            .setHeader("X-Session-Token", constant(Map.of("not", "published")))
                            .to(kafka).id("publish-order");
                    from("direct:dynamic").routeId("orders-dynamic")
                            .toD("kafka:${header.topic}?brokers=" + brokers).id("publish-dynamic");
                    from("direct:filtered").routeId("orders-filtered")
                            .to(kafka + "&headerFilterStrategy=#dropAuthorization").id("publish-filtered");
                }
            });
            context.getRegistry().bind("dropAuthorization", new KafkaHeaderFilterStrategy() {
                {
                    getOutFilter().add("authorization");
                }
            });
            context.start();
            try (var producer = context.createProducerTemplate()) {
                for (String route : List.of("direct:intake", "direct:intake", "direct:dynamic", "direct:filtered")) {
                    producer.send(route, e -> {
                        e.getMessage().setHeader("topic", topic);
                        e.getMessage().setHeader("Authorization", "Bearer synthetic-caller");
                        e.getMessage().setHeader("X-Api-Key", "synthetic-key".getBytes(StandardCharsets.UTF_8));
                        e.getMessage().setHeader("CamelHttpPassword", "synthetic-internal");
                        e.getMessage().setHeader(PlatformHeaders.CORRELATION_ID, "order-1");
                        e.getMessage().setBody("{}");
                    });
                }
            }

            var warnings = logs.at(Level.WARNING).stream().map(LogCapture::text).toList();
            assertEquals(java.util.stream.Stream.of(
                    "publish-order orders-intake Authorization", "publish-order orders-intake X-Api-Key",
                    "publish-dynamic orders-dynamic Authorization", "publish-dynamic orders-dynamic X-Api-Key",
                    "publish-filtered orders-filtered X-Api-Key").sorted().toList(),
                    warnings.stream().map(PublishedCredentialHeadersTest::summary).sorted().toList(),
                    "Once per route and header name; not for Camel's own headers, filtered ones or values "
                            + "the producer cannot write");
            assertEquals("Kafka send publish-order in route orders-intake publishes header Authorization. Its name "
                    + "marks it as a credential, and anyone who can read the topic can read the value. Remove the "
                    + "header before the send, for example with removeHeader(\"Authorization\"). A route that "
                    + "received the message over HTTP should remove the caller's headers with removeHeaders(\"*\") "
                    + "and then set only the headers the message needs",
                    warnings.stream().filter(w -> w.contains("orders-intake publishes header Authorization")).findFirst().orElseThrow());
            assertTrue(warnings.stream().noneMatch(w -> w.contains("synthetic")), "The value is never logged");
        }

        var records = consume(brokers, topic, 4);
        for (var record : records.subList(0, 3)) {
            assertEquals("Bearer synthetic-caller", header(record, "Authorization"), "Published as before");
            assertArrayEquals("synthetic-key".getBytes(StandardCharsets.UTF_8), record.headers().lastHeader("X-Api-Key").value());
        }
        assertEquals(null, records.getFirst().headers().lastHeader("X-Session-Token"), "A map value is not published");
        assertEquals(null, records.get(3).headers().lastHeader("Authorization"), "The endpoint's own filter applies");
    }

    /** Step, route and header name of one warning. */
    private static String summary(String warning) {
        String step = warning.substring("Kafka send ".length(), warning.indexOf(" in route "));
        String route = warning.substring(warning.indexOf(" in route ") + " in route ".length(), warning.indexOf(" publishes header "));
        String header = warning.substring(warning.indexOf(" publishes header ") + " publishes header ".length(), warning.indexOf(". Its name"));
        return step + " " + route + " " + header;
    }

    private static List<ConsumerRecord<String, String>> consume(String brokers, String topic, int expected) {
        try (var consumer = new KafkaConsumer<String, String>(Map.of("bootstrap.servers", brokers,
                "group.id", "verify-" + UUID.randomUUID(), "auto.offset.reset", "earliest"),
                new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(topic));
            var records = new ArrayList<ConsumerRecord<String, String>>();
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (records.size() < expected && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(200)).forEach(records::add);
            }
            assertEquals(expected, records.size());
            return records;
        }
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        var header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static final class NoStepTracker implements StepTracker {
        public void beforeSend(Exchange e) {}
        public void recordDelivered(Exchange e) {}
        public void recordSkipped(Exchange e) {}
        public void recordFailed(Exchange e) {}
        public void recordFailedAndRethrow(Exchange e) {}
    }
}
