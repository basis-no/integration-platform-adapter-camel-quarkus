package no.basis.integrationplatform.camel.step;

import no.basis.integrationplatform.camel.LogCapture;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.PublishedCredentialHeaders;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaHeaderFilterStrategy;
import org.apache.camel.component.kafka.serde.KafkaHeaderSerializer;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultHeaderFilterStrategy;
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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
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
    void warnsOncePerRouteAndHeaderNameForWhatWasPublishedAndPublishesAsBefore() throws Exception {
        String topic = "intake-" + UUID.randomUUID();
        String brokers = KAFKA.getBootstrapServers();
        String kafka = "kafka:" + topic + "?brokers=" + brokers;
        try (var logs = LogCapture.of(PublishedCredentialHeaders.class);
             var admin = AdminClient.create(Map.<String, Object>of("bootstrap.servers", brokers));
             var context = new DefaultCamelContext()) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(20, TimeUnit.SECONDS);
            context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(new NoStepTracker()));
            // Camel's own strategy class, configured: evaluated from its configuration.
            var dropAuthorization = new DefaultHeaderFilterStrategy();
            dropAuthorization.setOutFilterStartsWith("Camel", "camel", "org.apache.camel.", "kafka.");
            dropAuthorization.setOutFilter(Set.of("authorization"));
            dropAuthorization.setLowerCase(true);
            context.getRegistry().bind("dropAuthorization", dropAuthorization);
            // Application classes: never called by the check, so such a send is not checked.
            context.getRegistry().bind("customFilter", new KafkaHeaderFilterStrategy() {
                {
                    getOutFilter().add("authorization");
                }
            });
            context.getRegistry().bind("customSerializer", (KafkaHeaderSerializer) (key, value) ->
                    value instanceof byte[] bytes ? bytes : String.valueOf(value).getBytes(StandardCharsets.UTF_8));
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:intake").routeId("orders-intake")
                            .setHeader("X-Session-Token", constant(Map.of("not", "published")))
                            .to(kafka).id("publish-order");
                    from("direct:dynamic").routeId("orders-dynamic")
                            .toD("kafka:${header.topic}?brokers=" + brokers).id("publish-dynamic");
                    from("direct:filtered").routeId("orders-filtered")
                            .to(kafka + "&headerFilterStrategy=#dropAuthorization").id("publish-filtered");
                    from("direct:dynamic-filtered").routeId("orders-dynamic-filtered")
                            .toD("kafka:${header.topic}?brokers=" + brokers + "&headerFilterStrategy=#dropAuthorization")
                            .id("publish-dynamic-filtered");
                    from("direct:scheme-at-send-time").routeId("orders-any-scheme")
                            .toD("${header.target}").id("publish-any");
                    from("direct:custom-filter").routeId("orders-custom-filter")
                            .to(kafka + "&headerFilterStrategy=#customFilter").id("publish-custom-filter");
                    from("direct:custom-serializer").routeId("orders-custom-serializer")
                            .to(kafka + "&headerSerializer=#customSerializer").id("publish-custom-serializer");
                    from("direct:many").routeId("orders-many")
                            .to(kafka).id("publish-many");
                }
            });
            context.start();
            List<String> routes = List.of("direct:intake", "direct:intake", "direct:dynamic", "direct:filtered",
                    "direct:dynamic-filtered", "direct:scheme-at-send-time", "direct:custom-filter",
                    "direct:custom-serializer");
            try (var producer = context.createProducerTemplate()) {
                for (String route : routes) {
                    producer.send(route, e -> {
                        e.getMessage().setHeader("topic", topic);
                        e.getMessage().setHeader("target", kafka);
                        e.getMessage().setHeader("Authorization", "Bearer synthetic-caller");
                        e.getMessage().setHeader("X-Api-Key", "synthetic-key".getBytes(StandardCharsets.UTF_8));
                        e.getMessage().setHeader("CamelHttpPassword", "synthetic-internal");
                        e.getMessage().setHeader(PlatformHeaders.CORRELATION_ID, "order-1");
                        e.getMessage().setBody("{}");
                    });
                }
                producer.send("direct:many", e -> {
                    for (int i = 0; i < 101; i++) {
                        e.getMessage().setHeader("X-Token-" + i, "synthetic-" + i);
                    }
                    e.getMessage().setBody("{}");
                });
            }

            var warnings = logs.at(Level.WARNING).stream().map(LogCapture::text).toList();
            assertEquals(Stream.of(
                    "publish-order orders-intake Authorization", "publish-order orders-intake X-Api-Key",
                    "publish-dynamic orders-dynamic Authorization", "publish-dynamic orders-dynamic X-Api-Key",
                    "publish-filtered orders-filtered X-Api-Key",
                    "publish-dynamic-filtered orders-dynamic-filtered X-Api-Key",
                    // A send that picks its component at send time withholds a record's byte headers.
                    "publish-any orders-any-scheme Authorization").sorted().toList(),
                    warnings.stream().filter(w -> !w.contains("orders-many"))
                            .map(PublishedCredentialHeadersTest::summary).sorted().toList(),
                    "Once per route and header name; not for Camel's own headers, filtered ones, values the "
                            + "producer cannot write, or sends with application filter or serializer classes");
            assertEquals("Kafka send publish-order in route orders-intake published header Authorization. Its name "
                    + "marks it as a credential, and anyone who can read the topic can read the value. Remove the "
                    + "header before the send, for example with removeHeader(\"Authorization\"). A route that "
                    + "received the message over HTTP should remove the caller's headers with removeHeaders(\"*\") "
                    + "and then set only the headers the message needs",
                    warnings.stream().filter(w -> w.contains("orders-intake published header Authorization"))
                            .findFirst().orElseThrow());
            var many = warnings.stream().filter(w -> w.contains("orders-many")).toList();
            assertEquals(101, many.size(), "100 names, then one notice");
            assertEquals("Kafka sends in route orders-many published credential-like headers under 100 different "
                    + "names; further names in this route are not logged", many.getLast());
            assertTrue(warnings.stream().noneMatch(w -> w.contains("synthetic")), "The value is never logged");
        }

        var records = consume(brokers, topic, 9);
        for (var record : records.subList(0, 3)) {
            assertEquals("Bearer synthetic-caller", header(record, "Authorization"), "Published as before");
            assertArrayEquals("synthetic-key".getBytes(StandardCharsets.UTF_8), record.headers().lastHeader("X-Api-Key").value());
        }
        assertNull(records.getFirst().headers().lastHeader("X-Session-Token"), "A map value is not published");
        assertNull(records.get(3).headers().lastHeader("Authorization"), "The endpoint's own filter applies");
        assertNull(records.get(4).headers().lastHeader("Authorization"), "Also the filter a toD resolved");
        assertNull(records.get(5).headers().lastHeader("X-Api-Key"), "Withheld before a send to a scheme chosen at send time");
        assertEquals("Bearer synthetic-caller", header(records.get(7), "Authorization"),
                "Published through an application serializer, which the check does not call");
    }

    /**
     * A filter built from Camel's class can still hold an application collection, which may
     * answer differently on each call. The check must not call it: two sends publish the same
     * with and without the check.
     */
    @Test
    void anApplicationCollectionInTheFilterIsNeverCalled() throws Exception {
        try (var logs = LogCapture.of(PublishedCredentialHeaders.class)) {
            var unchecked = publishTwice(false);
            var checked = publishTwice(true);
            assertEquals(unchecked, checked, "Same records and the same calls on the collection");
            assertTrue(logs.at(Level.WARNING).isEmpty(), "Not checked");
        }
    }

    /** Whether each record carries Authorization, then how often the producer asked the collection. */
    private static List<Object> publishTwice(boolean check) throws Exception {
        var calls = new TogglingSet();
        String topic = "toggling-" + UUID.randomUUID();
        String brokers = KAFKA.getBootstrapServers();
        try (var admin = AdminClient.create(Map.<String, Object>of("bootstrap.servers", brokers));
             var context = new DefaultCamelContext()) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(20, TimeUnit.SECONDS);
            if (check) {
                context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(new NoStepTracker()));
            }
            var toggling = new DefaultHeaderFilterStrategy();
            toggling.setOutFilterStartsWith("Camel", "camel", "org.apache.camel.", "kafka.");
            toggling.setOutFilter(calls);
            context.getRegistry().bind("toggling", toggling);
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:publish").routeId("orders-toggling")
                            .to("kafka:" + topic + "?brokers=" + brokers + "&headerFilterStrategy=#toggling").id("publish");
                }
            });
            context.start();
            try (var producer = context.createProducerTemplate()) {
                for (int i = 0; i < 2; i++) {
                    producer.sendBodyAndHeader("direct:publish", "{}", "Authorization", "Bearer synthetic-caller");
                }
            }
        }
        var result = new ArrayList<Object>();
        consume(brokers, topic, 2).forEach(r -> result.add(r.headers().lastHeader("Authorization") != null));
        result.add(calls.calls());
        return result;
    }

    /** Holds Authorization on every second call, whether Camel asks with contains or iterates. */
    private static final class TogglingSet extends java.util.HashSet<String> {
        private int calls;

        @Override
        public synchronized boolean contains(Object name) {
            return calls++ % 2 == 1 && "Authorization".equalsIgnoreCase(String.valueOf(name));
        }

        @Override
        public synchronized java.util.Iterator<String> iterator() {
            return (calls++ % 2 == 1 ? List.of("Authorization") : List.<String>of()).iterator();
        }

        synchronized int calls() {
            return calls;
        }
    }

    /**
     * A toD with cacheSize=-1 creates an endpoint for the send without registering it (Camel's
     * prototype endpoint), so the check cannot find it afterwards; Camel reuses an endpoint that
     * is already registered, which is checked.
     */
    @Test
    void aToDWithoutEndpointCacheIsCheckedOnlyThroughARegisteredEndpoint() throws Exception {
        try (var logs = LogCapture.of(PublishedCredentialHeaders.class);
             var context = new DefaultCamelContext()) {
            context.start();
            var check = new PublishedCredentialHeaders();
            var prototype = org.apache.camel.support.CamelContextHelper.getMandatoryPrototypeEndpoint(
                    context, "kafka:prototype?brokers=localhost:1");
            var registered = context.getEndpoint("kafka:registered?brokers=localhost:1");
            for (var endpoint : List.of(prototype, registered)) {
                // What a toD leaves on the exchange after its send.
                var exchange = exchange(context, "Authorization");
                exchange.setProperty(org.apache.camel.ExchangePropertyKey.TO_ENDPOINT, endpoint.getEndpointUri());
                check.check(exchange, null, "orders-" + endpoint.getEndpointBaseUri().substring("kafka://".length()), "publish");
            }
            assertNull(context.hasEndpoint(prototype.getEndpointUri()), "Camel did not register the prototype");
            assertEquals(List.of("orders-registered"), logs.at(Level.WARNING).stream()
                    .map(w -> LogCapture.text(w).split(" ")[5]).toList(), "Only the registered endpoint is checked");
        }
    }

    @Test
    void theBoundPerRouteHoldsUnderConcurrentSends() throws Exception {
        try (var logs = LogCapture.of(PublishedCredentialHeaders.class);
             var context = new DefaultCamelContext()) {
            context.start();
            var endpoint = context.getEndpoint("kafka:bound?brokers=localhost:1");
            var check = new PublishedCredentialHeaders();
            for (int i = 0; i < 99; i++) {
                check.check(exchange(context, "X-Token-" + i), endpoint, "orders-bound", "publish");
            }
            var pool = java.util.concurrent.Executors.newFixedThreadPool(16);
            var start = new java.util.concurrent.CountDownLatch(1);
            var sends = new ArrayList<java.util.concurrent.Future<?>>();
            for (int i = 99; i < 163; i++) {
                String name = "X-Token-" + i;
                sends.add(pool.submit(() -> {
                    start.await();
                    check.check(exchange(context, name), endpoint, "orders-bound", "publish");
                    return null;
                }));
            }
            start.countDown();
            for (var send : sends) {
                send.get(10, TimeUnit.SECONDS);
            }
            pool.shutdown();
            var warnings = logs.at(Level.WARNING).stream().map(LogCapture::text).toList();
            assertEquals(100, warnings.stream().filter(w -> w.contains(" published header ")).count());
            assertEquals(1, warnings.stream().filter(w -> w.contains("further names in this route are not logged")).count());
        }
    }

    private static Exchange exchange(DefaultCamelContext context, String header) {
        var exchange = new org.apache.camel.support.DefaultExchange(context);
        exchange.getMessage().setHeader(header, "synthetic");
        return exchange;
    }

    /** Step, route and header name of one warning. */
    private static String summary(String warning) {
        String step = warning.substring("Kafka send ".length(), warning.indexOf(" in route "));
        String route = warning.substring(warning.indexOf(" in route ") + " in route ".length(), warning.indexOf(" published header "));
        String header = warning.substring(warning.indexOf(" published header ") + " published header ".length(), warning.indexOf(". Its name"));
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
