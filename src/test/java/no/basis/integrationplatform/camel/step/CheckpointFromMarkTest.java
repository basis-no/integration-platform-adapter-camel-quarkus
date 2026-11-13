package no.basis.integrationplatform.camel.step;

import com.sun.net.httpserver.HttpServer;
import no.basis.integrationplatform.camel.PlatformDeadLetterRoute;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.PlatformRouteConfiguration;
import no.basis.integrationplatform.camel.lifecycle.MessageStoreRoutePolicy;
import no.basis.integrationplatform.camel.lifecycle.PayloadCapturePolicy;
import no.basis.integrationplatform.camel.lifecycle.PayloadMode;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.model.StepStatus;
import no.basis.integrationplatform.camel.payload.PayloadObjectStore;
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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A checkpointed step whose DELIVERED outcome and output checkpoint could not be written: the
 * next read of the record writes both from the mark, does not call the receiver again, and
 * restores the step's output for the rest of the route, as a ledger skip does.
 */
@Timeout(60)
class CheckpointFromMarkTest {
    private static final String STEP = "create-shipment";
    private static final String KEY = "message-1:" + STEP;

    private final Map<String, StepOutputCheckpoint> delivered = new ConcurrentHashMap<>();
    private final List<MessageAttemptResult> rows = Collections.synchronizedList(new ArrayList<>());
    private final List<String> idempotencyKeys = Collections.synchronizedList(new ArrayList<>());
    private final List<String> outputs = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger commits = new AtomicInteger();
    private final AtomicBoolean readOnly = new AtomicBoolean(true);
    private HttpServer receiver;
    private DefaultCamelContext context;

    private final StepOutcomeStore store = new StepOutcomeStore() {
        public boolean supportsOutputCheckpoints() { return true; }
        public boolean wasDelivered(String key) { return delivered.containsKey(key); }
        public void recordDelivered(StepResult step) { throw new AssertionError("an output step needs its checkpoint"); }
        public void recordDelivered(StepResult step, StepOutputCheckpoint checkpoint) {
            if (readOnly.get()) {
                throw new StepOutcomeStoreException("read-only",
                        new java.sql.SQLException("cannot execute INSERT in a read-only transaction", "25006"));
            }
            delivered.put(step.idempotencyKey(), checkpoint);
        }
        public Optional<StepOutputCheckpoint> readCheckpoint(String key) { return Optional.ofNullable(delivered.get(key)); }
        public void recordFailed(StepResult step) { }
    };

    @BeforeEach
    void start() throws Exception {
        receiver = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        receiver.createContext("/", exchange -> {
            exchange.getRequestBody().readAllBytes();
            String key = exchange.getRequestHeaders().getFirst(PlatformHeaders.HTTP_IDEMPOTENCY_KEY);
            idempotencyKeys.add(key);
            byte[] body = ("shipment-for-" + key).getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        receiver.start();
        context = new DefaultCamelContext();
        var properties = new Properties();
        properties.setProperty("basis.message-store.dlq-uri", "direct:parking");
        context.getPropertiesComponent().setOverrideProperties(properties);
        var checkpoints = new StepOutputCheckpointService(
                PayloadObjectStore.disabled(),
                new PayloadCapturePolicy(PayloadMode.POSTGRES_INLINE, 1024, 10_000, Duration.ofDays(30), true),
                "shipping", "", "");
        DefaultStepTracker tracker = new DefaultStepTracker(store, new DefaultStepClassifier("", "", "", STEP), checkpoints);
        tracker.setRetrySleeper(millis -> true);
        context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(tracker));
        context.addRoutes(new PlatformRouteConfiguration());
        context.addRoutes(new PlatformDeadLetterRoute());
        String url = "http://127.0.0.1:" + receiver.getAddress().getPort() + "/shipments";
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                from("direct:parking").routeId("parking").errorHandler(noErrorHandler()).log("parked");
                from("direct:input").routeId("input")
                        .routePolicy(new MessageStoreRoutePolicy(rows::add, "shipping"))
                        .setHeader(Exchange.HTTP_METHOD, constant("POST"))
                        .to(url).id(STEP)
                        .process(e -> outputs.add(e.getMessage().getBody(String.class)));
            }
        });
        context.start();
    }

    @AfterEach
    void stop() throws Exception {
        if (context != null) context.close();
        receiver.stop(0);
    }

    @Test
    void theNextReadWritesTheCheckpointFromTheMarkAndRestoresTheOutputWithoutCallingAgain() {
        consume();
        assertEquals(List.of(KEY), idempotencyKeys, "The receiver answered once");
        assertEquals(0, commits.get(), "Not committed: the outcome was not written");
        assertTrue(outputs.isEmpty(), "The route did not get past the step");

        readOnly.set(false);
        consume();

        assertEquals(List.of(KEY), idempotencyKeys, "The receiver is not called again");
        assertNotNull(delivered.get(KEY), "The checkpoint is written from the mark");
        assertEquals(List.of("shipment-for-" + KEY), outputs, "The step's output is restored for the route");
        assertEquals(1, commits.get());
        assertEquals(MessageStatus.COMPLETED, rows.getLast().status());
    }

    private void consume() {
        try (var producer = context.createProducerTemplate()) {
            producer.send("direct:input", e -> {
                e.getMessage().setHeader(KafkaConstants.TOPIC, "bip.shipping.in");
                e.getMessage().setHeader(KafkaConstants.PARTITION, 0);
                e.getMessage().setHeader(KafkaConstants.OFFSET, 5L);
                e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, (KafkaManualCommit) commits::incrementAndGet);
                e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-1");
                e.getMessage().setBody("order");
            });
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
