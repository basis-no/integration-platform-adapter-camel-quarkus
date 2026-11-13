package no.basis.integrationplatform.camel.step;

import no.basis.integrationplatform.camel.LogCapture;
import no.basis.integrationplatform.camel.PlatformDeadLetterRoute;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.PlatformRouteConfiguration;
import no.basis.integrationplatform.camel.ReceiverComponent;
import no.basis.integrationplatform.camel.lifecycle.MessageStoreRoutePolicy;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.store.MessageStoreSinkException;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The log lines for a record whose step outcome or outcome could not be written, or whose own
 * data the store cannot hold, escape the record's message id and the step ids they name, as
 * every other line that writes a value from the record does: a CR/LF cannot start a forged line
 * and a bidirectional control cannot reorder the text.
 */
class StoreFailureLogEscapingTest {
    private static final String MESSAGE_ID = "message-1\r\n2026-01-01 INFO forged‮";
    private static final String ESCAPED_ID = "message-1\\u000d\\u000a2026-01-01 INFO forged\\u202e";
    private static final String STEP = "post order";
    private static final String ESCAPED_STEP = "post\\u2028order";

    private final ReceiverComponent receiver = new ReceiverComponent();
    private final Set<String> delivered = Collections.synchronizedSet(new HashSet<>());
    private final List<Exchange> parked = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger commits = new AtomicInteger();
    private final AtomicInteger deliveredWritesToFail = new AtomicInteger();
    private volatile SQLException deliveredWriteFailure;
    private volatile SQLException sinkFailure;
    private DefaultCamelContext context;

    @AfterEach
    void stop() throws Exception {
        if (context != null) {
            context.close();
        }
    }

    @Test
    void theStepTrackersLinesForAnUnwrittenOutcomeEscapeTheMessageIdAndTheStep() throws Exception {
        start();
        deliveredWriteFailure = new SQLException("read-only", "25006");

        try (var log = LogCapture.of(DefaultStepTracker.class)) {
            // Every try of the DELIVERED write fails: the outcome is kept for the record.
            deliveredWritesToFail.set(Integer.MAX_VALUE);
            read(7);
            // The next read of the same record writes it and does not send again.
            deliveredWritesToFail.set(0);
            read(7);
            // Another record whose write fails once is written when tried again in place.
            deliveredWritesToFail.set(1);
            read(8);

            assertEquals(2, receiver.received().size(), "Each record is sent once");
            List<String> lines = log.records().stream().map(LogCapture::text).toList();
            assertEscaped(lines, "was sent in an earlier read of record");
            assertEscaped(lines, "written when tried again in place");
        }
    }

    @Test
    void theRouteLinesForARecordTheStoreCannotHoldEscapeTheMessageIdAndTheStep() throws Exception {
        start();
        deliveredWriteFailure = new SQLException("index row size 2712 exceeds btree version 4 maximum 2704", "54000");
        deliveredWritesToFail.set(Integer.MAX_VALUE);

        try (var log = LogCapture.of(MessageStoreRoutePolicy.class)) {
            read(7);

            assertEquals(1, parked.size());
            assertEquals(1, commits.get());
            List<String> errors = log.at(Level.SEVERE).stream().map(LogCapture::text).toList();
            String line = assertEscaped(errors, "storeError=record-data");
            assertTrue(line.contains("the send of step " + ESCAPED_STEP + " had returned"), line);
        }
        // The reason is the adapter's own error text, which names the step: it is escaped too.
        String reason = parked.getFirst().getMessage().getHeader(PlatformHeaders.ERROR, String.class);
        assertTrue(reason.contains("the send of step " + ESCAPED_STEP + " had returned"), reason);
    }

    @Test
    void theLineForAnOutcomeThatCouldNotBeRecordedEscapesTheMessageId() throws Exception {
        start();
        sinkFailure = new SQLException("connection refused", "08001");

        try (var log = LogCapture.of(MessageStoreRoutePolicy.class)) {
            read(7);

            assertEquals(0, commits.get());
            assertEscaped(log.at(Level.SEVERE).stream().map(LogCapture::text).toList(),
                    "its outcome could not be recorded");
        }
    }

    /** The one line that contains the marker names the escaped id and holds no raw control. */
    private static String assertEscaped(List<String> lines, String marker) {
        List<String> matching = lines.stream().filter(line -> line.contains(marker)).toList();
        assertEquals(1, matching.size(), marker + " in " + lines);
        String line = matching.getFirst();
        assertTrue(line.contains(ESCAPED_ID), "The message id is escaped: " + line);
        assertFalse(line.contains("\r") || line.contains("\n") || line.contains("‮") || line.contains(" "),
                "No raw control: " + line);
        return line;
    }

    private void start() throws Exception {
        StepOutcomeStore store = new StepOutcomeStore() {
            public boolean wasDelivered(String key) { return delivered.contains(key); }
            public void recordDelivered(StepResult step) {
                if (deliveredWritesToFail.getAndDecrement() > 0) {
                    throw new StepOutcomeStoreException("write failed", deliveredWriteFailure);
                }
                delivered.add(step.idempotencyKey());
            }
            public void recordFailed(StepResult step) { }
        };
        DefaultStepTracker tracker = new DefaultStepTracker(store, new DefaultStepClassifier(Optional.empty(),
                Optional.empty(), Optional.of(STEP), Optional.empty()));
        tracker.setRetrySleeper(millis -> true);
        context = new DefaultCamelContext();
        var properties = new Properties();
        properties.setProperty("basis.message-store.dlq-uri", "direct:parking");
        context.getPropertiesComponent().setOverrideProperties(properties);
        context.addComponent("receiver", receiver);
        context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(tracker));
        context.addRoutes(new PlatformRouteConfiguration());
        context.addRoutes(new PlatformDeadLetterRoute());
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                from("direct:parking").routeId("parking").errorHandler(noErrorHandler()).process(e -> parked.add(e.copy()));
                from("direct:input").routeId("input").routeConfigurationId(PlatformRouteConfiguration.NO_RETRY)
                        .routePolicy(new MessageStoreRoutePolicy(result -> {
                            SQLException failure = sinkFailure;
                            if (failure != null) {
                                throw new MessageStoreSinkException("write failed", failure);
                            }
                        }, "order-pipeline"))
                        .to("receiver:orders").id(STEP);
            }
        });
        context.start();
    }

    private void read(long offset) {
        try (var producer = context.createProducerTemplate()) {
            producer.send("direct:input", e -> {
                e.getMessage().setHeader(KafkaConstants.TOPIC, "bip.order-pipeline.in");
                e.getMessage().setHeader(KafkaConstants.PARTITION, 0);
                e.getMessage().setHeader(KafkaConstants.OFFSET, offset);
                e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, (KafkaManualCommit) commits::incrementAndGet);
                e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, (MESSAGE_ID + offset).getBytes(StandardCharsets.UTF_8));
                e.getMessage().setBody("<Order/>");
            });
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
