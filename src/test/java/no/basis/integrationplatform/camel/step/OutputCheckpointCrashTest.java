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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The process dies after an output-producing receiver answered and before the step's
 * outcome and output checkpoint were written. The crash is modelled as a checkpoint write
 * that never returns, a hard stop of that CamelContext, and a new CamelContext over the
 * same stores, as a restarted pod would see them: nothing was committed or recorded, so the
 * record is read again and the step runs again with the same {@code Idempotency-Key}. Only once its output is
 * checkpointed does a replay skip the call and restore the output.
 */
@Timeout(60)
class OutputCheckpointCrashTest {
    private static final String STEP = "create-shipment";
    private static final String KEY = "message-1:" + STEP;

    private final Map<String, StepOutputCheckpoint> delivered = new ConcurrentHashMap<>();
    private final List<MessageAttemptResult> rows = Collections.synchronizedList(new ArrayList<>());
    private final List<String> idempotencyKeys = Collections.synchronizedList(new ArrayList<>());
    private final List<String> outputs = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger commits = new AtomicInteger();
    private final AtomicBoolean crashBeforeCheckpoint = new AtomicBoolean();
    private final CountDownLatch dying = new CountDownLatch(1);
    private HttpServer receiver;
    private Thread dyingConsumer;
    private DefaultCamelContext context;

    /** Behaves like the Postgres store, and can die before the delivered row is written. */
    private final StepOutcomeStore store = new StepOutcomeStore() {
        public boolean supportsOutputCheckpoints() { return true; }
        public boolean wasDelivered(String key) { return delivered.containsKey(key); }
        public void recordDelivered(StepResult step) { throw new AssertionError("an output step needs its checkpoint"); }
        public void recordDelivered(StepResult step, StepOutputCheckpoint checkpoint) {
            if (crashBeforeCheckpoint.get()) {
                // The process dies here: the write never returns and nothing after it runs.
                dying.countDown();
                try {
                    new CountDownLatch(1).await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                throw new IllegalStateException("process died before the checkpoint was written");
            }
            delivered.put(step.idempotencyKey(), checkpoint);
        }
        public Optional<StepOutputCheckpoint> readCheckpoint(String key) { return Optional.ofNullable(delivered.get(key)); }
        public void recordFailed(StepResult step) { }
    };

    @BeforeEach
    void startReceiver() throws Exception {
        receiver = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        receiver.createContext("/", exchange -> {
            exchange.getRequestBody().readAllBytes();
            String key = exchange.getRequestHeaders().getFirst(PlatformHeaders.HTTP_IDEMPOTENCY_KEY);
            idempotencyKeys.add(key);
            // An output-producing receiver answers a repeated key with the same output.
            byte[] body = ("shipment-for-" + key).getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        receiver.start();
    }

    @AfterEach
    void stop() throws Exception {
        if (dyingConsumer != null) dyingConsumer.interrupt();
        if (context != null) context.close();
        receiver.stop(0);
    }

    @Test
    void aCrashBeforeTheCheckpointRunsTheStepAgainWithTheSameKey() throws Exception {
        crashBeforeCheckpoint.set(true);
        start();
        dyingConsumer = new Thread(this::consume);
        dyingConsumer.start();
        assertTrue(dying.await(30, TimeUnit.SECONDS), "The receiver answered and the checkpoint write began");

        // The state the process dies in, observed while the write is still blocked.
        assertTrue(dyingConsumer.isAlive());
        assertEquals(List.of(KEY), idempotencyKeys, "The receiver answered once");
        assertEquals(0, commits.get(), "Nothing is committed while the checkpoint is not written");
        assertTrue(delivered.isEmpty(), "No delivered row and no checkpoint");
        assertTrue(outputs.isEmpty(), "The route did not get past the step");

        context.getShutdownStrategy().setTimeout(1);
        context.getShutdownStrategy().setShutdownNowOnTimeout(true);
        context.close();
        dyingConsumer.interrupt();
        dyingConsumer.join(10_000);
        assertFalse(dyingConsumer.isAlive(), "The dying consumer has stopped");
        assertEquals(List.of(KEY), idempotencyKeys, "Dying made no further call");
        assertEquals(0, commits.get(), "Nor a commit");
        assertTrue(delivered.isEmpty(), "Nor a delivered row");

        // The pod restarts: a new context over the same stores reads the record again.
        crashBeforeCheckpoint.set(false);
        start();
        consume();

        assertEquals(List.of(KEY, KEY), idempotencyKeys, "The step runs again, with the same Idempotency-Key");
        assertEquals(1, commits.get());
        assertNotNull(delivered.get(KEY), "Its output is checkpointed this time");
        assertEquals(List.of("shipment-for-" + KEY), outputs);
        assertEquals(MessageStatus.COMPLETED, rows.getLast().status());
        assertEquals(List.of(StepStatus.DELIVERED), statuses(rows.getLast()));

        // A replay skips the call and restores the checkpointed output.
        consume();

        assertEquals(2, idempotencyKeys.size(), "The checkpointed step is not called again");
        assertEquals(List.of("shipment-for-" + KEY, "shipment-for-" + KEY), outputs);
        assertEquals(List.of(StepStatus.SKIPPED), statuses(rows.getLast()));
    }

    private static List<StepStatus> statuses(MessageAttemptResult row) {
        return row.steps().stream().map(StepResult::status).toList();
    }

    private void start() throws Exception {
        context = new DefaultCamelContext();
        var properties = new Properties();
        properties.setProperty("basis.message-store.dlq-uri", "direct:parking");
        context.getPropertiesComponent().setOverrideProperties(properties);
        var checkpoints = new StepOutputCheckpointService(
                PayloadObjectStore.disabled(),
                new PayloadCapturePolicy(PayloadMode.POSTGRES_INLINE, 1024, 10_000, Duration.ofDays(30), true),
                "shipping", "", "");
        context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(new DefaultStepTracker(
                store, new DefaultStepClassifier("", "", "", STEP), checkpoints)));
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

    private void consume() {
        try (var producer = context.createProducerTemplate()) {
            producer.send("direct:input", e -> {
                e.getMessage().setHeader(KafkaConstants.TOPIC, "bip.shipping.in");
                e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, (KafkaManualCommit) commits::incrementAndGet);
                e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-1");
                e.getMessage().setBody("order");
            });
        } catch (Exception e) {
            if (!crashBeforeCheckpoint.get()) {
                throw new AssertionError(e);
            }
        }
    }
}
