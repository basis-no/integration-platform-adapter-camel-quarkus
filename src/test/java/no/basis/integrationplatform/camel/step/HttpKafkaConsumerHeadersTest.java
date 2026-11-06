package no.basis.integrationplatform.camel.step;

import com.sun.net.httpserver.HttpServer;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Camel 4.18.0's Kafka consumer sets {@code kafka.HEADERS}, {@code kafka.TOPIC} and the other
 * {@code kafka.*} headers; HTTP producers sent them, and {@code kafka.HEADERS} listed every
 * record header's bytes. They are withheld at the HTTP endpoint the send resolved to, so a
 * {@code toD} URI and a route behind an internal endpoint still read them, and every EIP that
 * sends to HTTP is covered.
 */
class HttpKafkaConsumerHeadersTest {
    private static final byte[] CALLER_AUTHORIZATION = "Bearer synthetic-caller".getBytes(StandardCharsets.UTF_8);

    private final List<Request> requests = Collections.synchronizedList(new ArrayList<>());
    private final List<Object> seen = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger failures = new AtomicInteger();
    private final RecordHeaders recordHeaders = new RecordHeaders();
    private volatile CountDownLatch received = new CountDownLatch(0);
    private HttpServer server;
    private DefaultCamelContext context;
    private String receiver;

    private record Request(String path, Set<String> headerNames, String routeHeader) {
    }

    @BeforeEach
    void start() throws Exception {
        recordHeaders.add("Authorization", CALLER_AUTHORIZATION);
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", x -> {
            x.getRequestBody().readAllBytes();
            var names = new TreeSet<String>();
            x.getRequestHeaders().keySet().forEach(name -> names.add(name.toLowerCase(Locale.ROOT)));
            requests.add(new Request(x.getRequestURI().getPath(), names, x.getRequestHeaders().getFirst("X-Route-Header")));
            x.sendResponseHeaders(failures.getAndDecrement() > 0 ? 500 : 200, -1);
            x.close();
            received.countDown();
        });
        server.start();
        receiver = "http://127.0.0.1:" + server.getAddress().getPort();
        context = new DefaultCamelContext();
        context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(new NoStepTracker()));
    }

    @AfterEach
    void stop() throws Exception {
        context.close();
        server.stop(0);
    }

    @Test
    void aToDUriAndARouteBehindAnInternalEndpointStillReadTheHeaders() throws Exception {
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                from("direct:record").routeId("record")
                        .to(receiver + "/static").id("post-static")
                        .process(e -> seen.add(e.getMessage().getHeader("kafka.HEADERS")))
                        .toD(receiver + "/${headerAs(kafka.TOPIC,java.lang.String)}").id("post-topic")
                        .toD("${header.targetUri}").id("send-target")
                        .to("direct:sub");
                from("direct:sub").routeId("sub")
                        .process(e -> seen.add(e.getMessage().getHeader("kafka.TOPIC")));
            }
        });
        context.start();
        sendRecord(Map.of("targetUri", "direct:sub"));

        assertEquals(List.of("/static", "/orders-in"), requests.stream().map(Request::path).toList(),
                "A toD URI that reads a kafka.* header sends where it did before");
        assertNoConsumerHeaders();
        assertEquals(3, seen.size(), seen.toString());
        assertSame(recordHeaders, seen.get(0), "The route still sees the headers after an HTTP send");
        assertEquals(List.of("orders-in", "orders-in"), seen.subList(1, 3),
                "A route behind a static or dynamic internal send still sees them");
    }

    @Test
    void everyEipThatSendsToHttpWithholdsThem() throws Exception {
        received = new CountDownLatch(6);
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                from("direct:record").routeId("record")
                        .enrich(receiver + "/enrich")
                        .wireTap(receiver + "/tap")
                        .recipientList(constant(receiver + "/list"))
                        .routingSlip(constant(receiver + "/slip"))
                        .multicast().to(receiver + "/multicast").id("post-multicast").end()
                        .process(e -> e.getContext().createProducerTemplate().send(receiver + "/template", e))
                        .process(e -> seen.add(e.getMessage().getHeader("kafka.HEADERS")));
            }
        });
        context.start();
        sendRecord(Map.of());

        assertTrue(received.await(10, TimeUnit.SECONDS), "Received: " + requests);
        assertEquals(Set.of("/enrich", "/tap", "/list", "/slip", "/multicast", "/template"),
                Set.copyOf(requests.stream().map(Request::path).toList()));
        assertNoConsumerHeaders();
        assertSame(recordHeaders, seen.getFirst(), "The route still sees the headers after the sends");
    }

    @Test
    void aFailedSendRestoresTheHeadersForItsRedelivery() throws Exception {
        failures.set(1);
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                from("direct:record").routeId("record")
                        .errorHandler(defaultErrorHandler().maximumRedeliveries(1).redeliveryDelay(0))
                        .toD(receiver + "/${headerAs(kafka.TOPIC,java.lang.String)}").id("post-topic")
                        .process(e -> seen.add(e.getMessage().getHeader("kafka.HEADERS")));
            }
        });
        context.start();
        Exchange result = sendRecord(Map.of());

        assertEquals(null, result.getException());
        assertEquals(List.of("/orders-in", "/orders-in"), requests.stream().map(Request::path).toList(),
                "The redelivery sends where the first try did");
        assertNoConsumerHeaders();
        assertSame(recordHeaders, seen.getFirst());
    }

    private void assertNoConsumerHeaders() {
        for (Request request : requests) {
            assertTrue(request.headerNames().stream().noneMatch(name -> name.startsWith("kafka.")),
                    request.path() + " got " + request.headerNames());
            assertEquals("route-value", request.routeHeader(), "Other headers are sent as before");
        }
    }

    private Exchange sendRecord(Map<String, Object> extra) {
        try (var producer = context.createProducerTemplate()) {
            return producer.send("direct:record", e -> {
                // As Camel 4.18.0's Kafka consumer sets them; later versions name them CamelKafka*.
                e.getMessage().setHeader("kafka.HEADERS", recordHeaders);
                e.getMessage().setHeader("kafka.TOPIC", "orders-in");
                e.getMessage().setHeader("kafka.KEY", "record-key");
                e.getMessage().setHeader("kafka.OFFSET", 42L);
                e.getMessage().setHeader("X-Route-Header", "route-value");
                e.getMessage().getHeaders().putAll(extra);
                e.getMessage().setBody("{}");
            });
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static final class NoStepTracker implements StepTracker {
        public void beforeSend(Exchange e) {}
        public void recordDelivered(Exchange e) {}
        public void recordSkipped(Exchange e) {}
        public void recordFailed(Exchange e) {}
        public void recordFailedAndRethrow(Exchange e) {}
    }
}
