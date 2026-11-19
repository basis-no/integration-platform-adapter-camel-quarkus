package no.basis.integrationplatform.camel.lifecycle;

import com.sun.net.httpserver.HttpServer;
import no.basis.integrationplatform.camel.PlatformDeadLetterRoute;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.PlatformRouteConfiguration;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.step.DefaultStepTracker;
import no.basis.integrationplatform.camel.step.StepOutcomeStore;
import no.basis.integrationplatform.camel.step.StepTrackingInterceptStrategy;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A tracked {@code toD} whose target is known only when it sends records nothing for itself
 * when it resolves to an internal endpoint, such as a {@code direct:} route, also when that route
 * makes its sends in split parts and parks one; a {@code toD} that resolves to an outside receiver is recorded,
 * also next to one that runs a route in a parallel multicast.
 */
@Timeout(60)
class DynamicSendStepOutcomeTest {
    private final Map<String, String> outcomes = new ConcurrentHashMap<>();
    private final List<MessageAttemptResult> rows = Collections.synchronizedList(new ArrayList<>());
    private final List<String> parked = Collections.synchronizedList(new ArrayList<>());
    private final Map<String, AtomicInteger> calls = new ConcurrentHashMap<>();
    private volatile int ordersStatus = 500;
    private HttpServer receiver;
    private DefaultCamelContext context;

    /** Behaves like the Postgres store: FAILED never replaces DELIVERED. */
    private final StepOutcomeStore store = new StepOutcomeStore() {
        public boolean wasDelivered(String key) { return "DELIVERED".equals(outcomes.get(key)); }
        public void recordDelivered(StepResult step) { outcomes.put(Objects.requireNonNull(step.idempotencyKey()), "DELIVERED"); }
        public void recordFailed(StepResult step) {
            outcomes.compute(Objects.requireNonNull(step.idempotencyKey()),
                    (key, status) -> "DELIVERED".equals(status) ? status : "FAILED");
        }
    };

    @BeforeEach
    void startReceiver() throws Exception {
        receiver = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        receiver.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            calls.computeIfAbsent(path, p -> new AtomicInteger()).incrementAndGet();
            exchange.getRequestBody().readAllBytes();
            exchange.sendResponseHeaders(path.equals("/orders") ? ordersStatus : 200, -1);
            exchange.close();
        });
        receiver.start();
    }

    @AfterEach
    void stop() throws Exception {
        if (context != null) context.close();
        receiver.stop(0);
    }

    @Test
    void aToDIntoARouteThatSendsFromSplitPartsIsNotSkippedOnTheResend() throws Exception {
        start(new RouteBuilder() {
            public void configure() {
                from("direct:input").routeId("input")
                        .routePolicy(new MessageStoreRoutePolicy(rows::add, "order-pipeline"))
                        .setHeader("next", constant("direct:deliver"))
                        .toD("${header.next}").id("dispatch");
                from("direct:deliver").routeId("deliver")
                        .split(body().tokenize(","))
                            .setHeader(Exchange.HTTP_METHOD, constant("POST"))
                            .to(url("/orders")).id("post-order")
                        .end();
            }
        });

        read("message-1", "order");

        assertEquals(4, calls("/orders"), "The send and its three redeliveries");
        assertEquals(List.of("order"), parked, "The split part was parked");
        assertEquals(Map.of("message-1:post-order", "FAILED"), outcomes, "The toD recorded nothing");

        ordersStatus = 200;
        read("message-1", "order");

        assertEquals(5, calls("/orders"), "The resend ran the route, and its part sent again");
        assertEquals(Map.of("message-1:post-order", "DELIVERED"), outcomes);
    }

    @Test
    void aParallelToDToAReceiverIsRecordedNextToOneThatRunsARoute() throws Exception {
        ordersStatus = 200;
        start(new RouteBuilder() {
            public void configure() {
                from("direct:input").routeId("input")
                        .routePolicy(new MessageStoreRoutePolicy(rows::add, "order-pipeline"))
                        .setHeader("next", constant("direct:deliver"))
                        .setHeader("receipts", constant(url("/receipts")))
                        .setHeader(Exchange.HTTP_METHOD, constant("POST"))
                        .multicast().parallelProcessing()
                            .toD("${header.next}").id("dispatch")
                            .toD("${header.receipts}").id("post-receipt")
                        .end();
                from("direct:deliver").routeId("deliver")
                        .split(body().tokenize(","))
                            .to(url("/orders")).id("post-order")
                        .end();
            }
        });

        for (int i = 1; i <= 20; i++) {
            read("message-" + i, "order");
        }

        for (int i = 1; i <= 20; i++) {
            assertEquals("DELIVERED", outcomes.get("message-" + i + ":post-receipt"),
                    "The toD to the receiver is recorded: " + outcomes);
            assertEquals("DELIVERED", outcomes.get("message-" + i + ":post-order"));
            assertFalse(outcomes.containsKey("message-" + i + ":dispatch"), "The toD into the route records nothing");
        }
        assertEquals(20, calls("/receipts"));
        assertEquals(20, calls("/orders"));

        for (int i = 1; i <= 20; i++) {
            read("message-" + i, "order");
        }

        assertEquals(20, calls("/receipts"), "A resend does not send the recorded receipt again");
        assertEquals(20, calls("/orders"), "nor the route's delivered part");
    }

    private int calls(String path) {
        AtomicInteger count = calls.get(path);
        return count == null ? 0 : count.get();
    }

    private String url(String path) {
        return "http://127.0.0.1:" + receiver.getAddress().getPort() + path;
    }

    private void start(RouteBuilder routes) throws Exception {
        context = new DefaultCamelContext();
        var properties = new Properties();
        properties.setProperty("basis.message-store.dlq-uri", "direct:parking");
        context.getPropertiesComponent().setOverrideProperties(properties);
        context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(new DefaultStepTracker(store)));
        context.addRoutes(new PlatformRouteConfiguration());
        context.addRoutes(new PlatformDeadLetterRoute());
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                from("direct:parking").routeId("parking").errorHandler(noErrorHandler())
                        .process(e -> parked.add(e.getMessage().getBody(String.class)));
            }
        });
        context.addRoutes(routes);
        context.start();
    }

    private void read(String messageId, String body) {
        try (var producer = context.createProducerTemplate()) {
            producer.send("direct:input", e -> {
                e.getMessage().setHeader(KafkaConstants.TOPIC, "bip.order-pipeline.in");
                e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, (KafkaManualCommit) () -> { });
                e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, messageId);
                e.getMessage().setBody(body);
            });
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
