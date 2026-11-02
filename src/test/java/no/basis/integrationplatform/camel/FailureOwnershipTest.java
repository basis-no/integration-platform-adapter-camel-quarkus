package no.basis.integrationplatform.camel;

import no.basis.integrationplatform.camel.lifecycle.MessageStoreRoutePolicy;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import no.basis.integrationplatform.camel.step.StepTracker;
import no.basis.integrationplatform.camel.step.StepTrackingInterceptStrategy;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.direct.DirectConsumerNotAvailableException;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Who owns a terminal failure on a Kafka input: the real Camel error handler and route-policy
 * callbacks, without a broker. The input commits only after the outcome is durable, and a
 * {@code DLQ} outcome only after the dead-letter endpoint accepted the record.
 */
class FailureOwnershipTest {
    @Test
    void adapterOwnedFailureParksOriginalInputThenCommits() throws Exception {
        var outcome = run(false, false, false);
        assertEquals(List.of("<Input/>"), outcome.parked);
        assertEquals(1, outcome.rows.size());
        assertEquals(MessageStatus.DLQ, outcome.rows.getFirst().status());
        assertEquals("synthetic receiver rejection", outcome.rows.getFirst().error());
        assertEquals(1, outcome.commits.get());
        assertNull(outcome.exchange.getException(), "A parked failure is handled");
    }

    @Test
    void dlqPublicationFailureMustNotCommitOrClaimParked() throws Exception {
        var outcome = run(false, true, false);
        assertTrue(outcome.parked.isEmpty());
        assertEquals(1, outcome.rows.size());
        assertAll(
                () -> assertEquals(0, outcome.commits.get(), "The record was not parked, so its offset must not be committed"),
                () -> assertEquals(MessageStatus.FAILED, outcome.rows.getFirst().status()),
                // The Kafka consumer reads a record again only when its exchange failed.
                () -> assertNotNull(outcome.exchange.getException(), "The exchange must stay failed"));
        assertEquals("synthetic DLQ outage", outcome.exchange.getException().getMessage());
    }

    @Test
    void applicationCatchAllBypassesAdapterParkingAndDoesNotCommit() throws Exception {
        var outcome = run(true, false, false);
        assertEquals(6, outcome.calls.get(), "One initial call and the application's five redeliveries");
        assertTrue(outcome.parked.isEmpty(), "Application onException bypasses the adapter dead-letter handler");
        assertEquals(MessageStatus.FAILED, outcome.rows.getFirst().status());
        assertEquals(0, outcome.commits.get());
        assertInstanceOf(IllegalArgumentException.class, outcome.exchange.getException());
    }

    @Test
    void applicationHandlerThatHandlesTheFailureCompletesTheMessage() throws Exception {
        Result result = new Result();
        try (var context = context(result, false)) {
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    onException(Exception.class).handled(true).process(e -> { });
                    from("direct:input").routeId("input").routeConfigurationId(PlatformRouteConfiguration.NO_RETRY)
                            .routePolicy(new MessageStoreRoutePolicy(result.rows::add, "order-pipeline"))
                            .process(e -> {
                                throw new IllegalArgumentException("synthetic receiver rejection");
                            });
                }
            });
            context.start();
            send(context, result);
        }
        assertTrue(result.parked.isEmpty());
        assertEquals(MessageStatus.COMPLETED, result.rows.getFirst().status());
        assertEquals(1, result.commits.get());
    }

    @ParameterizedTest
    @ValueSource(strings = {"${header.missingDlqUri}", "direct:no-consumer?failIfNoConsumers=false",
            "file:target/dead-letters", "kafka:dlq?requestRequiredAcks=0", "kafka:dlq?additionalProperties.acks=0",
            "kafka:dlq?transacted=true", "kafka:dlq?transactionalId=parking",
            "kafka:dlq?additionalProperties.transactional.id=parking"})
    void dlqUriWhoseSuccessDoesNotConfirmParkingFailsStartup(String dlqUri) throws Exception {
        try (var context = new DefaultCamelContext()) {
            var properties = new Properties();
            properties.setProperty("basis.message-store.dlq-uri", dlqUri);
            context.getPropertiesComponent().setOverrideProperties(properties);
            context.addRoutes(new PlatformRouteConfiguration());
            context.addRoutes(new PlatformDeadLetterRoute());
            Exception thrown = assertThrows(Exception.class, context::start);
            Throwable cause = thrown;
            while (cause.getCause() != null && !(cause instanceof IllegalArgumentException)) cause = cause.getCause();
            assertInstanceOf(IllegalArgumentException.class, cause);
            assertTrue(cause.getMessage().startsWith("basis.message-store.dlq-uri must"), cause.getMessage());
        }
    }

    @Test
    void directDlqWithoutConsumerIsNotParkedOrCommitted() throws Exception {
        var outcome = run(false, false, false, "direct:no-consumer?block=false");
        assertTrue(outcome.parked.isEmpty());
        assertEquals(0, outcome.commits.get());
        assertEquals(MessageStatus.FAILED, outcome.rows.getFirst().status());
        assertInstanceOf(DirectConsumerNotAvailableException.class, outcome.exchange.getException());
    }

    @Test
    void parkingRouteThatStopsStillCountsAsParked() throws Exception {
        var outcome = run(false, false, false, "direct:parking-then-stop");
        assertEquals(List.of("<Input/>"), outcome.parked);
        assertEquals(MessageStatus.DLQ, outcome.rows.getFirst().status());
        assertEquals(1, outcome.commits.get());
    }

    @Test
    void journalFailureDoesNotCommit() throws Exception {
        var outcome = run(false, false, true);
        assertEquals(0, outcome.commits.get());
        assertNotNull(outcome.exchange.getException());
    }

    @Test
    void successIsRecordedAndCommittedOnce() throws Exception {
        Result result = new Result();
        try (var context = context(result, false)) {
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:input").routeId("input").routeConfigurationId(PlatformRouteConfiguration.NO_RETRY)
                            .routePolicy(new MessageStoreRoutePolicy(result.rows::add, "order-pipeline"))
                            .process(e -> result.calls.incrementAndGet());
                }
            });
            context.start();
            send(context, result);
        }
        assertEquals(1, result.calls.get());
        assertTrue(result.parked.isEmpty());
        assertEquals(MessageStatus.COMPLETED, result.rows.getFirst().status());
        assertEquals(1, result.commits.get());
        assertNull(result.exchange.getException());
    }

    @Test
    void anApplicationWithoutKafkaStartsWithTheDeadLetterRoute() throws Exception {
        try (var context = new DefaultCamelContext()) {
            context.addRoutes(new PlatformRouteConfiguration());
            context.addRoutes(new PlatformDeadLetterRoute());
            context.start();
            assertTrue(context.isStarted());
        }
    }

    private Result run(boolean appHandler, boolean dlqFailure, boolean journalFailure) throws Exception {
        return run(appHandler, dlqFailure, journalFailure, null);
    }

    private Result run(boolean appHandler, boolean dlqFailure, boolean journalFailure, String dlqUriAtStart)
            throws Exception {
        Result result = new Result();
        try (var context = context(result, dlqFailure)) {
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    if (appHandler) {
                        onException(Exception.class).maximumRedeliveries(5).redeliveryDelay(0)
                                .handled(false).process(e -> { });
                    }
                    // An explicit policy stands in for the Kafka lifecycle on a local source.
                    from("direct:input").routeId("input").routeConfigurationId(PlatformRouteConfiguration.NO_RETRY)
                            .routePolicy(new MessageStoreRoutePolicy(row -> {
                                if (journalFailure) throw new IllegalStateException("synthetic journal outage");
                                result.rows.add(row);
                            }, "order-pipeline"))
                            .process(e -> {
                                result.calls.incrementAndGet();
                                e.getMessage().setBody("<Transformed/>");
                                throw new IllegalArgumentException("synthetic receiver rejection");
                            });
                }
            });
            if (dlqUriAtStart != null) {
                var properties = new Properties();
                properties.setProperty("basis.message-store.dlq-uri", dlqUriAtStart);
                context.getPropertiesComponent().setOverrideProperties(properties);
            }
            context.start();
            send(context, result);
        }
        return result;
    }

    private DefaultCamelContext context(Result result, boolean dlqFailure) throws Exception {
        var context = new DefaultCamelContext();
        var properties = new Properties();
        properties.setProperty("basis.message-store.dlq-uri", "direct:parking");
        context.getPropertiesComponent().setOverrideProperties(properties);
        context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(new StepTracker() {
            public void beforeSend(Exchange e) { fail("Parking is not a business step"); }
            public void recordDelivered(Exchange e) { }
            public void recordSkipped(Exchange e) { }
            public void recordFailed(Exchange e) { }
            public void recordFailedAndRethrow(Exchange e) { }
        }, false));
        context.addRoutes(new PlatformRouteConfiguration());
        context.addRoutes(new PlatformDeadLetterRoute());
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                from("direct:parking").routeId("parking").errorHandler(noErrorHandler())
                        .process(e -> {
                            if (dlqFailure) throw new IllegalStateException("synthetic DLQ outage");
                            result.parked.add(e.getMessage().getBody(String.class));
                        });
                from("direct:parking-then-stop").routeId("parking-then-stop").errorHandler(noErrorHandler())
                        .process(e -> result.parked.add(e.getMessage().getBody(String.class)))
                        .stop();
            }
        });
        return context;
    }

    private static void send(DefaultCamelContext context, Result result) {
        try (var producer = context.createProducerTemplate()) {
            result.exchange = producer.request("direct:input", e -> {
                e.getMessage().setBody("<Input/>");
                e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT,
                        (KafkaManualCommit) result.commits::incrementAndGet);
            });
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static class Result {
        final List<MessageAttemptResult> rows = new ArrayList<>();
        final List<String> parked = new ArrayList<>();
        final AtomicInteger commits = new AtomicInteger();
        final AtomicInteger calls = new AtomicInteger();
        Exchange exchange;
    }
}
