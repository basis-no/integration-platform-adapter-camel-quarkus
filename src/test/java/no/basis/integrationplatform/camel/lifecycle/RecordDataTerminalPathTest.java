package no.basis.integrationplatform.camel.lifecycle;

import no.basis.integrationplatform.camel.PlatformDeadLetterRoute;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.PlatformRouteConfiguration;
import no.basis.integrationplatform.camel.ReceiverComponent;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.step.DefaultStepClassifier;
import no.basis.integrationplatform.camel.step.DefaultStepTracker;
import no.basis.integrationplatform.camel.step.StepOutcomeStore;
import no.basis.integrationplatform.camel.step.StepOutcomeStoreException;
import no.basis.integrationplatform.camel.step.StepTrackingInterceptStrategy;
import no.basis.integrationplatform.camel.store.MessageStoreSinkException;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.model.RouteDefinition;
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
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A record whose own values the Message Store cannot hold is parked and recorded without them,
 * and committed, instead of blocking its partition (#138).
 */
class RecordDataTerminalPathTest {
    private final List<MessageAttemptResult> rows = Collections.synchronizedList(new ArrayList<>());
    private final List<Exchange> parked = Collections.synchronizedList(new ArrayList<>());
    private final ReceiverComponent orders = new ReceiverComponent();
    private final ReceiverComponent invoices = new ReceiverComponent();
    private final AtomicInteger commits = new AtomicInteger();
    private final Set<String> delivered = new HashSet<>();
    private volatile SQLException deliveredWriteFails;
    private volatile SQLException checkFails;
    private volatile SQLException sinkFailsOnce;
    private volatile String refuseRowsWithCorrelation;
    private volatile boolean refuseReducedRowsWithPayload;
    private DefaultCamelContext context;

    @AfterEach
    void stop() throws Exception {
        if (context != null) {
            context.close();
        }
    }

    @Test
    void aNulInTheCorrelationIdIsParkedAndCommittedWithoutRunningTheRoute() throws Exception {
        start(route -> route.to("receiver:orders").id("post-order"));

        Exchange read = read(7, "message-1", "order\u0000-1");

        assertEquals(0, orders.received().size(), "The route did not run");
        assertEquals(1, parked.size());
        assertEquals("message-1", parked.getFirst().getMessage().getHeader(PlatformHeaders.MESSAGE_ID));
        String reason = parked.getFirst().getMessage().getHeader(PlatformHeaders.ERROR, String.class);
        assertTrue(reason.contains("the record's envelope: x-bip-correlation-id contains a NUL byte"), reason);
        assertTrue(reason.endsWith("(record bip.order-pipeline.in/0@7)"), "The reason names the record: " + reason);
        assertFalse(reason.contains("\u0000") || reason.contains("-1"), "The refused value is not copied: " + reason);
        MessageAttemptResult row = rows.getLast();
        assertEquals("message-1", row.messageId());
        assertEquals(MessageStatus.DLQ, row.status());
        assertNull(row.correlationId());
        assertEquals(List.of(), row.steps());
        assertEquals(reason, row.error());
        assertEquals(1, commits.get());
        assertNull(read.getException(), "The consumer goes on with the next record");
    }

    @Test
    void aMessageIdTheStoreCannotHoldIsParkedUnderTheIdDerivedFromItsPosition() throws Exception {
        start(route -> route.to("receiver:orders").id("post-order"));

        read(42, "m".repeat(2_000), null);

        String derived = KafkaRecordIdentity.derive("order-pipeline", "bip.order-pipeline.in", 0, 42);
        assertEquals(derived, parked.getFirst().getMessage().getHeader(PlatformHeaders.MESSAGE_ID));
        assertEquals(derived, rows.getLast().messageId());
        assertTrue(rows.getLast().error().contains("x-bip-message-id is 2000 bytes, over the 1024-byte limit"),
                rows.getLast().error());
        assertEquals(0, orders.received().size());
        assertEquals(1, commits.get());
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {
            PlatformHeaders.CONTENT_TYPE, PlatformHeaders.SOURCE_SYSTEM, PlatformHeaders.TARGET_SYSTEM})
    void aNulInAnyOtherStoredHeaderIsParkedWithoutRunningTheRoute(String header) throws Exception {
        start(route -> route.to("receiver:orders").id("post-order"));

        read(7, "message-1", null, java.util.Map.of(header, "a\u0000b".getBytes(StandardCharsets.UTF_8)),
                commits::incrementAndGet);

        assertEquals(0, orders.received().size(), "The route did not run");
        assertEquals(1, parked.size());
        assertTrue(rows.getLast().error().contains(header + " contains a NUL byte"), rows.getLast().error());
        assertEquals(1, commits.get());
    }

    @Test
    void aNulInTheMessageIdIsParkedUnderTheIdDerivedFromItsPosition() throws Exception {
        start(route -> route.to("receiver:orders").id("post-order"));

        read(3, "message\u0000-1", null);

        String derived = KafkaRecordIdentity.derive("order-pipeline", "bip.order-pipeline.in", 0, 3);
        assertEquals(derived, parked.getFirst().getMessage().getHeader(PlatformHeaders.MESSAGE_ID));
        assertEquals(derived, rows.getLast().messageId());
        assertTrue(rows.getLast().error().contains("x-bip-message-id contains a NUL byte"), rows.getLast().error());
        assertEquals(0, orders.received().size());
        assertEquals(1, commits.get());
    }

    @Test
    void aRefusedRecordWhosePayloadCannotBeCapturedIsStillRefusedAndParkedAsItCame() throws Exception {
        start(route -> route.to("receiver:orders").id("post-order"));

        // A claim check that cannot be read, on a record the boundary check refuses.
        Exchange read = read(7, "message-1", "order\u0000-1",
                java.util.Map.of(PlatformHeaders.PAYLOAD_STORAGE, "s3_object"), commits::incrementAndGet);

        assertEquals(0, orders.received().size(), "The route did not run");
        assertEquals(1, parked.size());
        assertEquals("<Order/>", parked.getFirst().getMessage().getBody(String.class), "Parked as it came");
        assertEquals(MessageStatus.DLQ, rows.getLast().status());
        assertEquals("metadata_only", rows.getLast().payloadStorage());
        assertEquals(1, commits.get());
        assertNull(read.getException());
    }

    @Test
    void aRecordWhoseCommitFailsAfterItWasParkedIsNotParkedAgain() throws Exception {
        start(route -> route.to("receiver:orders").id("post-order"));
        AtomicInteger commitTries = new AtomicInteger();
        KafkaManualCommit failsOnce = () -> {
            if (commitTries.incrementAndGet() == 1) {
                throw new IllegalStateException("commit failed");
            }
            commits.incrementAndGet();
        };

        Exchange first = read(7, "message-1", "order\u0000-1", java.util.Map.of(), failsOnce);
        assertNotNull(first.getException(), "Not committed: read again");
        read(7, "message-1", "order\u0000-1", java.util.Map.of(), failsOnce);

        assertEquals(1, parked.size(), "Parked once");
        assertEquals(1, commits.get());
    }

    @Test
    void aStepOutcomeTheStoreCannotHoldParksTheRecordAlsoWhenTheRouteCaughtIt() throws Exception {
        deliveredWriteFails = new SQLException("index row size 2712 exceeds btree version 4 maximum 2704", "54000");
        start(route -> route.doTry().to("receiver:orders").id("post-order").doCatch(Exception.class).end());

        Exchange read = read(7, "message-1", null);

        assertEquals(1, orders.received().size());
        assertEquals(1, parked.size());
        String reason = rows.getLast().error();
        assertTrue(reason.contains("SQLSTATE 54000") && reason.contains("the send of step post-order had returned"),
                reason);
        assertEquals(MessageStatus.DLQ, rows.getLast().status());
        assertEquals(1, commits.get());
        assertNull(read.getException());
    }

    @Test
    void noLaterSendOfAnAttemptIsMadeAfterAStepOutcomeCheckFailed() throws Exception {
        checkFails = new SQLException("connection refused", "08001");
        start(route -> route.doTry().to("receiver:orders").id("post-order").doCatch(Exception.class).end()
                .to("invoices:all").id("post-invoice"));

        Exchange read = read(7, "message-1", null);

        assertEquals(0, orders.received().size());
        assertEquals(0, invoices.received().size(), "The caught store failure ends the attempt");
        assertEquals(0, commits.get());
        assertNotNull(read.getException());
        assertEquals(0, parked.size());
    }

    @Test
    void anOutcomeWriteTheStoreCannotHoldRecordsTheCompletedMessageWithoutParkingIt() throws Exception {
        sinkFailsOnce = new SQLException("unsupported Unicode escape sequence", "22P05");
        start(route -> route.to("receiver:orders").id("post-order"));

        read(7, "message-1", null);

        assertEquals(1, orders.received().size());
        assertEquals(0, parked.size(), "Delivered: parking it would invite a resend");
        assertEquals(MessageStatus.COMPLETED, rows.getLast().status());
        assertEquals(List.of(), rows.getLast().steps());
        assertEquals(1, commits.get());
    }

    @Test
    void recordsTheBoundaryRefusedAreAllParkedHoweverManyInARow() throws Exception {
        start(route -> route.to("receiver:orders").id("post-order"));

        for (int offset = 1; offset <= 10; offset++) {
            read(offset, "message-" + offset, "order\u0000");
        }

        assertEquals(10, parked.size(), "A producer that sends a run of bad values does not block the partition");
        assertEquals(10, commits.get());
    }

    @Test
    void afterThreeRecordsInARowTheStoreRefusedTheNextIsHeldBack() throws Exception {
        refuseRowsWithCorrelation = "refused";
        start(route -> route.to("receiver:orders").id("post-order"));

        for (int offset = 1; offset <= 4; offset++) {
            read(offset, "message-" + offset, "refused");
        }

        assertEquals(3, commits.get(), "A setting or column that fails every record must not drain the topic");

        // A normal record resets the count.
        refuseRowsWithCorrelation = null;
        read(4, "message-4", "order-4");
        refuseRowsWithCorrelation = "refused";
        read(5, "message-5", "refused");
        assertEquals(5, commits.get());
    }

    @Test
    void theBoundIsOnBytes() throws Exception {
        start(route -> route.to("receiver:orders").id("post-order"));

        read(1, "message-1", "\u00e9".repeat(512));
        read(2, "message-2", "\u00e9".repeat(512) + "x");

        assertEquals(1, orders.received().size(), "1,024 bytes pass, 1,025 are refused");
        assertEquals(1, parked.size());
        assertTrue(rows.getLast().error().contains("is 1025 bytes"), rows.getLast().error());
    }

    @Test
    void aReducedOutcomeTheStoreRefusesTooIsWrittenWithoutThePayload() throws Exception {
        refuseReducedRowsWithPayload = true;
        start(route -> route.to("receiver:orders").id("post-order"));

        read(1, "message-1", "order\u0000");

        assertEquals(1, commits.get());
        assertEquals("metadata_only", rows.getLast().payloadStorage());
        assertNull(rows.getLast().payloadBytes());
        assertEquals(1, parked.size());
    }

    private void start(Consumer<RouteDefinition> body) throws Exception {
        StepOutcomeStore store = new StepOutcomeStore() {
            public boolean wasDelivered(String key) {
                if (checkFails != null) {
                    throw new StepOutcomeStoreException("check failed", checkFails);
                }
                return delivered.contains(key);
            }
            public void recordDelivered(StepResult step) {
                if (deliveredWriteFails != null) {
                    throw new StepOutcomeStoreException("write failed", deliveredWriteFails);
                }
                delivered.add(step.idempotencyKey());
            }
            public void recordFailed(StepResult step) { }
        };
        context = new DefaultCamelContext();
        var properties = new Properties();
        properties.setProperty("basis.message-store.dlq-uri", "direct:parking");
        context.getPropertiesComponent().setOverrideProperties(properties);
        context.addComponent("receiver", orders);
        context.addComponent("invoices", invoices);
        context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(new DefaultStepTracker(
                store, new DefaultStepClassifier(Optional.empty(), Optional.empty(),
                        Optional.of("post-order,post-invoice"), Optional.empty()))));
        context.addRoutes(new PlatformRouteConfiguration());
        context.addRoutes(new PlatformDeadLetterRoute());
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                from("direct:parking").routeId("parking").errorHandler(noErrorHandler()).process(e -> parked.add(e.copy()));
                body.accept(from("direct:input").routeId("input").routeConfigurationId(PlatformRouteConfiguration.NO_RETRY)
                        .routePolicy(new MessageStoreRoutePolicy(result -> {
                            SQLException failure = sinkFailsOnce;
                            if (failure != null) {
                                sinkFailsOnce = null;
                                throw new MessageStoreSinkException("write failed", failure);
                            }
                            if (refuseRowsWithCorrelation != null && refuseRowsWithCorrelation.equals(result.correlationId())) {
                                throw new MessageStoreSinkException("write failed",
                                        new SQLException("value too long", "22001"));
                            }
                            if (refuseReducedRowsWithPayload && result.correlationId() == null
                                    && result.payloadBytes() != null) {
                                throw new MessageStoreSinkException("write failed",
                                        new SQLException("invalid byte sequence", "22021"));
                            }
                            rows.add(result);
                        }, "order-pipeline")));
            }
        });
        context.start();
    }

    private Exchange read(long offset, String messageId, String correlationId) {
        return read(offset, messageId, correlationId, java.util.Map.of(), commits::incrementAndGet);
    }

    private Exchange read(long offset, String messageId, String correlationId, java.util.Map<String, Object> headers,
            KafkaManualCommit commit) {
        try (var producer = context.createProducerTemplate()) {
            return producer.send("direct:input", e -> {
                e.getMessage().setHeaders(new java.util.HashMap<>(headers));
                e.getMessage().setHeader(KafkaConstants.TOPIC, "bip.order-pipeline.in");
                e.getMessage().setHeader(KafkaConstants.PARTITION, 0);
                e.getMessage().setHeader(KafkaConstants.OFFSET, offset);
                e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, commit);
                e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, messageId.getBytes(StandardCharsets.UTF_8));
                if (correlationId != null) {
                    e.getMessage().setHeader(PlatformHeaders.CORRELATION_ID, correlationId.getBytes(StandardCharsets.UTF_8));
                }
                e.getMessage().setBody("<Order/>");
            });
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
