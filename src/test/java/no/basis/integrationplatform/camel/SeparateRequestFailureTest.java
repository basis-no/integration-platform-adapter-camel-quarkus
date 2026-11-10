package no.basis.integrationplatform.camel;

import no.basis.integrationplatform.camel.lifecycle.MessageStoreRoutePolicy;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
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
import java.util.function.Consumer;
import java.util.logging.Level;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Only a managed Kafka input is parked. An exchange of its own, such as a request a Kafka route
 * sends with a ProducerTemplate, or an input from another source, fails back to its caller.
 */
class SeparateRequestFailureTest {
    @Test
    void aRequestOnItsOwnExchangeFailsToItsCallerAndOnlyTheInputIsParked() throws Exception {
        var result = run(input -> input.process(e -> {
            Exchange reply = e.getContext().createProducerTemplate()
                    .request("direct:carrier", request -> request.getMessage().setBody("<Request/>"));
            if (reply.getException() != null) {
                throw reply.getException();
            }
        }), true);

        assertEquals(List.of("<Shipment/>"), result.parked, "Only the consumed input is parked");
        assertEquals(1, result.errors.get());
        assertEquals(List.of(MessageStatus.DLQ), result.statuses());
        assertEquals(1, result.commits.get());
    }

    @Test
    void aFailedRequestTheCallerIgnoresIsNotParked() throws Exception {
        var result = run(input -> input.process(e -> e.getContext().createProducerTemplate()
                .request("direct:carrier", request -> request.getMessage().setBody("<Request/>"))), true);

        assertTrue(result.parked.isEmpty(), "The request is not parked on its own");
        assertEquals(0, result.errors.get(), "Nothing is parked, so no ERROR says it is");
        assertEquals(List.of(MessageStatus.COMPLETED), result.statuses());
    }

    @Test
    void anInputFromAnotherSourceFailsBackToItsSource() throws Exception {
        var result = run(input -> input.to("direct:carrier"), false);

        assertTrue(result.parked.isEmpty());
        assertEquals(0, result.errors.get());
        assertInstanceOf(IllegalStateException.class, result.exchange.getException());
        assertEquals("carrier rejected", result.exchange.getException().getMessage());
    }

    @Test
    void aKafkaInputWithAFailingSubRouteOnItsOwnExchangeIsParkedOnce() throws Exception {
        var result = run(input -> input.to("direct:carrier").to("direct:carrier"), true);

        assertEquals(List.of("<Shipment/>"), result.parked);
        assertEquals(1, result.errors.get());
        assertEquals(List.of(MessageStatus.DLQ), result.statuses());
        assertEquals(1, result.commits.get());
    }

    private Result run(Consumer<org.apache.camel.model.RouteDefinition> steps, boolean kafkaInput) throws Exception {
        var result = new Result();
        try (var logs = LogCapture.of(PlatformRouteConfiguration.class); var context = new DefaultCamelContext()) {
            var properties = new Properties();
            properties.setProperty("basis.message-store.dlq-uri", "direct:parking");
            context.getPropertiesComponent().setOverrideProperties(properties);
            context.addRoutes(new PlatformRouteConfiguration());
            context.addRoutes(new PlatformDeadLetterRoute());
            context.addRoutes(new RouteBuilder() {
                @Override
                public void configure() {
                    var input = from("direct:input").routeId("input").routeConfigurationId(PlatformRouteConfiguration.NO_RETRY);
                    if (kafkaInput) {
                        input.routePolicy(new MessageStoreRoutePolicy(result.rows::add, "order-pipeline"));
                    }
                    steps.accept(input);
                    from("direct:carrier").routeId("carrier").routeConfigurationId(PlatformRouteConfiguration.NO_RETRY)
                            .process(e -> { throw new IllegalStateException("carrier rejected"); });
                    from("direct:parking").routeId("parking").errorHandler(noErrorHandler())
                            .process(e -> result.parked.add(e.getMessage().getBody(String.class)));
                }
            });
            context.start();
            try (var producer = context.createProducerTemplate()) {
                result.exchange = producer.request("direct:input", e -> {
                    e.getMessage().setBody("<Shipment/>");
                    e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "shipment-1");
                    if (kafkaInput) {
                        e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, (KafkaManualCommit) result.commits::incrementAndGet);
                    }
                });
            }
            result.errors.set(logs.at(Level.SEVERE).size());
        }
        return result;
    }

    private static final class Result {
        final List<MessageAttemptResult> rows = new ArrayList<>();
        final List<String> parked = new ArrayList<>();
        final AtomicInteger commits = new AtomicInteger();
        final AtomicInteger errors = new AtomicInteger();
        Exchange exchange;

        List<MessageStatus> statuses() {
            return rows.stream().map(MessageAttemptResult::status).toList();
        }
    }
}
