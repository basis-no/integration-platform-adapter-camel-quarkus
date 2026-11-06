package no.basis.integrationplatform.camel.lifecycle;

import com.sun.net.httpserver.HttpServer;
import no.basis.integrationplatform.camel.PlatformDeadLetterRoute;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.PlatformRouteConfiguration;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.model.StepStatus;
import no.basis.integrationplatform.camel.step.DefaultStepClassifier;
import no.basis.integrationplatform.camel.step.DefaultStepTracker;
import no.basis.integrationplatform.camel.step.StepOutcomeStore;
import no.basis.integrationplatform.camel.step.StepTrackingInterceptStrategy;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.model.RouteDefinition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A Kafka route that calls a {@code direct:} sub-route, whose tracked HTTP send fails until
 * the dead-letter channel parks the message: the sub-route's own, or the input route's when
 * the sub-route has no error handler. The outcome of the HTTP send is recorded once per
 * try: the parked message's step stays FAILED, so its resend sends it again, and a
 * successful send records one DELIVERED.
 */
@Timeout(60)
class SubRouteStepOutcomeTest {
    private static final String KEY = "message-1:post-order";

    private final Map<String, String> outcomes = new ConcurrentHashMap<>();
    private final List<MessageAttemptResult> rows = Collections.synchronizedList(new ArrayList<>());
    private final List<String> parked = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger commits = new AtomicInteger();
    private final AtomicInteger calls = new AtomicInteger();
    private volatile int receiverStatus = 500;
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
            calls.incrementAndGet();
            exchange.getRequestBody().readAllBytes();
            exchange.sendResponseHeaders(receiverStatus, -1);
            exchange.close();
        });
        receiver.start();
    }

    @AfterEach
    void stop() throws Exception {
        if (context != null) context.close();
        receiver.stop(0);
    }

    @ParameterizedTest
    @ValueSource(strings = {"direct", "direct-in-try", "sub-route-without-error-handler"})
    void aParkedSendInASubRouteStaysFailedAndItsResendSendsItAgain(String shape) throws Exception {
        start(shape);

        sendKafka();

        assertEquals(4, calls.get(), "The send and its three redeliveries");
        assertEquals(List.of("order"), parked);
        assertEquals(MessageStatus.DLQ, rows.getLast().status());
        assertEquals(1, commits.get());
        assertEquals(Map.of(KEY, "FAILED"), outcomes);
        assertEquals(List.of(StepStatus.FAILED, StepStatus.FAILED, StepStatus.FAILED, StepStatus.FAILED),
                statuses(rows.getLast()));

        receiverStatus = 200;
        sendKafka();

        assertEquals(5, calls.get(), "The resend sends the parked step again");
        assertEquals(MessageStatus.COMPLETED, rows.getLast().status());
        assertEquals(Map.of(KEY, "DELIVERED"), outcomes);
        assertEquals(List.of(StepStatus.DELIVERED), statuses(rows.getLast()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"direct", "direct-in-try", "sub-route-without-error-handler"})
    void aSuccessfulSendInASubRouteIsRecordedOnce(String shape) throws Exception {
        receiverStatus = 200;
        start(shape);

        sendKafka();

        assertEquals(1, calls.get());
        assertTrue(parked.isEmpty());
        assertEquals(MessageStatus.COMPLETED, rows.getLast().status());
        assertEquals(Map.of(KEY, "DELIVERED"), outcomes);
        assertEquals(List.of(StepStatus.DELIVERED), statuses(rows.getLast()));

        sendKafka();

        assertEquals(1, calls.get(), "A resend skips the delivered step");
        assertEquals(List.of(StepStatus.SKIPPED), statuses(rows.getLast()));
    }

    private static List<StepStatus> statuses(MessageAttemptResult row) {
        return row.steps().stream().map(StepResult::status).toList();
    }

    private void start(String shape) throws Exception {
        context = new DefaultCamelContext();
        var properties = new Properties();
        properties.setProperty("basis.message-store.dlq-uri", "direct:parking");
        context.getPropertiesComponent().setOverrideProperties(properties);
        context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(new DefaultStepTracker(store,
                new DefaultStepClassifier(Optional.empty(), Optional.empty(), Optional.of("post-order"), Optional.empty()))));
        context.addRoutes(new PlatformRouteConfiguration());
        context.addRoutes(new PlatformDeadLetterRoute());
        String url = "http://127.0.0.1:" + receiver.getAddress().getPort() + "/orders";
        Consumer<RouteDefinition> input = switch (shape) {
            case "direct", "sub-route-without-error-handler" -> route -> route.to("direct:deliver");
            case "direct-in-try" -> route -> route.doTry().to("direct:deliver").doCatch(Exception.class).log("caught").end();
            default -> throw new IllegalArgumentException(shape);
        };
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                from("direct:parking").routeId("parking").errorHandler(noErrorHandler())
                        .process(e -> parked.add(e.getMessage().getBody(String.class)));
                input.accept(from("direct:input").routeId("input")
                        .routePolicy(new MessageStoreRoutePolicy(rows::add, "order-pipeline")));
                RouteDefinition deliver = from("direct:deliver").routeId("deliver");
                if (shape.equals("sub-route-without-error-handler")) {
                    deliver.errorHandler(noErrorHandler());
                }
                deliver.setHeader(Exchange.HTTP_METHOD, constant("POST"))
                        .to(url).id("post-order");
            }
        });
        context.start();
    }

    private void sendKafka() {
        try (var producer = context.createProducerTemplate()) {
            producer.send("direct:input", e -> {
                e.getMessage().setHeader(KafkaConstants.TOPIC, "bip.order-pipeline.in");
                e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, (KafkaManualCommit) commits::incrementAndGet);
                e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-1");
                e.getMessage().setBody("order");
            });
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
