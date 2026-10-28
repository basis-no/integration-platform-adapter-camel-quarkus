package no.basis.integrationplatform.camel;

import no.basis.integrationplatform.camel.lifecycle.MessageStoreRoutePolicy;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import no.basis.integrationplatform.camel.step.StepTracker;
import no.basis.integrationplatform.camel.step.StepTrackingInterceptStrategy;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

/** No receiver/Kafka access: exercises the real Camel error handler and route-policy callbacks. */
class FailureOwnershipTest {
    @Test
    void adapterOwnedFailureParksOriginalInputThenCommits() throws Exception {
        var outcome = run(false, false, false);
        assertEquals(1, outcome.parked.size());
        assertEquals("<Input/>", outcome.parked.get(0));
        assertEquals(MessageStatus.DLQ, outcome.rows.get(0).status());
        assertEquals(1, outcome.commits.get());
    }

    @Test
    void exhaustedFailureIsLoggedOnceWithItsStackTraceBeforeParking() throws Exception {
        try (var logs = LogCapture.of(PlatformRouteConfiguration.class)) {
            var outcome = run(false, false, false);

            var errors = logs.at(java.util.logging.Level.SEVERE);
            assertEquals(1, errors.size());
            assertInstanceOf(IllegalArgumentException.class, errors.getFirst().getThrown());
            assertEquals("synthetic receiver rejection", errors.getFirst().getThrown().getMessage());
            assertEquals(MessageStatus.DLQ, outcome.rows.getFirst().status());
        }
    }

    @Test
    void applicationCatchAllBypassesPlatformParking() throws Exception {
        var outcome = run(true, false, false);
        assertEquals(6, outcome.calls.get(), "One initial call and the application's five redeliveries");
        assertTrue(outcome.parked.isEmpty(), "Application onException bypasses the adapter dead-letter handler");
        assertEquals(MessageStatus.FAILED, outcome.rows.get(0).status());
        assertEquals(0, outcome.commits.get());
    }

    @Test
    void journalFailureDoesNotCommit() throws Exception {
        assertEquals(0, run(false, false, true).commits.get());
    }

    @Test
    void dlqPublicationFailureMustNotCommitOrClaimParked() throws Exception {
        var outcome = run(false, true, false);
        assertEquals(0, outcome.commits.get());
        assertTrue(outcome.rows.stream().noneMatch(r -> r.status() == MessageStatus.DLQ));
    }

    @Test
    void freshInternalRequestFailurePropagatesToInputAndParksOnlyOriginal() throws Exception {
        Result result = new Result();
        try (var context = new DefaultCamelContext()) {
            var properties = new Properties();
            properties.setProperty("basis.message-store.dlq-uri", "direct:parking");
            context.getPropertiesComponent().setOverrideProperties(properties);
            context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(new StepTracker() {
                public void beforeSend(Exchange e) { fail("Infrastructure parking must not be a business step"); }
                public void recordDelivered(Exchange e) {}
                public void recordSkipped(Exchange e) {}
                public void recordFailed(Exchange e) {}
                public void recordFailedAndRethrow(Exchange e) {}
            }));
            context.addRoutes(new PlatformRouteConfiguration());
            context.addRoutes(new PlatformDeadLetterRoute());
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:input").routeConfigurationId(PlatformRouteConfiguration.NO_RETRY)
                        .routePolicy(new MessageStoreRoutePolicy(result.rows::add, "order-pipeline"))
                        .process(e -> {
                            try (var producer = context.createProducerTemplate()) {
                                var reply = producer.request("direct:receiver", child ->
                                        child.getMessage().setBody("<Transformed/>"));
                                assertNotNull(reply.getException(), "Internal error must reach caller");
                                assertInstanceOf(IllegalArgumentException.class, reply.getException());
                                assertEquals("synthetic receiver rejection", reply.getException().getMessage());
                                e.setException(reply.getException());
                            }
                        });
                    from("direct:receiver").routeConfigurationId(PlatformRouteConfiguration.NO_RETRY)
                        .process(e -> { throw new IllegalArgumentException("synthetic receiver rejection"); });
                    from("direct:parking").errorHandler(noErrorHandler())
                        .process(e -> result.parked.add(e.getMessage().getBody(String.class)));
                }
            });
            context.start();
            try (var producer = context.createProducerTemplate()) {
                producer.request("direct:input", e -> {
                    e.getMessage().setBody("<Input/>");
                    e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT,
                            (KafkaManualCommit) result.commits::incrementAndGet);
                });
            }
        }
        assertEquals(List.of("<Input/>"), result.parked);
        assertEquals(1, result.rows.size());
        assertEquals(MessageStatus.DLQ, result.rows.getFirst().status());
        assertEquals("synthetic receiver rejection", result.rows.getFirst().error());
        assertEquals(1, result.commits.get());
    }

    @Test
    void noKafkaApplicationCanStartWithoutBrokerConfiguration() throws Exception {
        try (var context = new DefaultCamelContext()) {
            context.addRoutes(new PlatformRouteConfiguration());
            context.addRoutes(new PlatformDeadLetterRoute());
            context.start();
            assertTrue(context.isStarted());
        }
    }

    private Result run(boolean appHandler, boolean dlqFailure, boolean journalFailure) throws Exception {
        Result result = new Result();
        try (var context = new DefaultCamelContext()) {
            var properties = new Properties();
            properties.setProperty("basis.message-store.dlq-uri", "direct:parking");
            context.getPropertiesComponent().setOverrideProperties(properties);
            context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(new StepTracker() {
                public void beforeSend(Exchange e) { fail("Infrastructure parking must not be a business step"); }
                public void recordDelivered(Exchange e) {}
                public void recordSkipped(Exchange e) {}
                public void recordFailed(Exchange e) {}
                public void recordFailedAndRethrow(Exchange e) {}
            }));
            context.addRoutes(new PlatformRouteConfiguration());
            context.addRoutes(new PlatformDeadLetterRoute());
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    if (appHandler) {
                        onException(Exception.class).maximumRedeliveries(5).redeliveryDelay(0)
                            .handled(false).process(e -> {});
                    }
                    // Explicit policy simulates the Kafka lifecycle while using a local source.
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
                    from("direct:parking").routeId("parking").errorHandler(noErrorHandler())
                        .process(e -> {
                            if (dlqFailure) throw new IllegalStateException("synthetic DLQ outage");
                            result.parked.add(e.getMessage().getBody(String.class));
                        });
                }
            });
            context.start();
            try (var producer = context.createProducerTemplate()) {
                producer.request("direct:input", e -> {
                    e.getMessage().setBody("<Input/>");
                    e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT,
                            (KafkaManualCommit) result.commits::incrementAndGet);
                });
            }
        }
        return result;
    }

    private static class Result {
        final List<MessageAttemptResult> rows = new ArrayList<>();
        final List<String> parked = new ArrayList<>();
        final AtomicInteger commits = new AtomicInteger();
        final AtomicInteger calls = new AtomicInteger();
    }
}
