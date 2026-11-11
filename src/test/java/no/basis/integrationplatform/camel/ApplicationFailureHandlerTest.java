package no.basis.integrationplatform.camel;

import no.basis.integrationplatform.camel.lifecycle.MessageStoreRoutePolicy;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.builder.RouteConfigurationBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Camel runs an error handler's onPrepareFailure also before an application's own onException
 * handler. Only the exchange the dead letter channel parks is prepared for parking; a handler
 * sees and sends the message as the route left it, as a doCatch does.
 */
class ApplicationFailureHandlerTest {
    @ParameterizedTest
    @ValueSource(strings = {"onException", "route onException", "route configuration onException", "doCatch"})
    void aHandledFailureSendsTheMessageAsTheRouteLeftIt(String handler) throws Exception {
        try (var logs = LogCapture.of(PlatformRouteConfiguration.class)) {
            var result = run(handler);

            assertLeftByTheRoute(result.received.get("compensate"));
            assertTrue(result.received.get("parking").isEmpty());
            assertEquals(MessageStatus.COMPLETED, result.rows.getFirst().status());
            assertEquals(1, result.commits.get());
            assertTrue(logs.at(Level.SEVERE).isEmpty(), "Nothing is sent to the dead letter queue");
        }
    }

    @Test
    void aContinuedFailureContinuesWithTheMessageAsTheRouteLeftIt() throws Exception {
        try (var logs = LogCapture.of(PlatformRouteConfiguration.class)) {
            var result = run("continued");

            assertLeftByTheRoute(result.received.get("compensate"));
            assertLeftByTheRoute(result.received.get("next"));
            assertTrue(result.received.get("parking").isEmpty());
            assertEquals(MessageStatus.COMPLETED, result.rows.getFirst().status());
            assertEquals(1, result.commits.get());
            assertTrue(logs.at(Level.SEVERE).isEmpty(), "Nothing is sent to the dead letter queue");
        }
    }

    @Test
    void aFailureTheHandlerDoesNotHandleIsNeitherParkedNorCommitted() throws Exception {
        try (var logs = LogCapture.of(PlatformRouteConfiguration.class)) {
            var result = run("not handled");

            assertLeftByTheRoute(result.received.get("compensate"));
            assertTrue(result.received.get("parking").isEmpty());
            assertEquals(MessageStatus.FAILED, result.rows.getFirst().status());
            assertEquals(0, result.commits.get());
            assertTrue(logs.at(Level.SEVERE).isEmpty(), "Nothing is sent to the dead letter queue");
        }
    }

    @Test
    void theAdapterDeadLetterChannelParksThePreparedInput() throws Exception {
        try (var logs = LogCapture.of(PlatformRouteConfiguration.class)) {
            var result = run("none");

            var parked = result.received.get("parking");
            assertEquals(1, parked.size());
            assertEquals("<Input/>", parked.getFirst().body());
            assertEquals(PlatformRouteConfiguration.MASKED, parked.getFirst().headers().get("Authorization"));
            assertEquals("message-1", parked.getFirst().headers().get(PlatformHeaders.MESSAGE_ID));
            assertEquals("synthetic receiver rejection", parked.getFirst().headers().get(PlatformHeaders.ERROR));
            assertNotNull(parked.getFirst().headers().get(PlatformHeaders.DLQ_AT));
            assertEquals(MessageStatus.DLQ, result.rows.getFirst().status());
            assertEquals(1, result.commits.get());
            assertEquals(1, logs.at(Level.SEVERE).size());
        }
    }

    private static void assertLeftByTheRoute(List<Received> received) {
        assertEquals(1, received.size());
        var message = received.getFirst();
        assertEquals("<Transformed/>", message.body());
        assertEquals("Bearer route-token", message.headers().get("Authorization"));
        assertEquals("message-2", message.headers().get(PlatformHeaders.MESSAGE_ID));
        assertNull(message.headers().get(PlatformHeaders.ERROR));
        assertNull(message.headers().get(PlatformHeaders.DLQ_AT));
    }

    private Result run(String handler) throws Exception {
        Result result = new Result();
        try (var context = new DefaultCamelContext()) {
            var properties = new Properties();
            properties.setProperty("basis.message-store.dlq-uri", "direct:parking");
            context.getPropertiesComponent().setOverrideProperties(properties);
            context.addRoutes(new PlatformRouteConfiguration());
            context.addRoutes(new RouteConfigurationBuilder() {
                public void configuration() {
                    routeConfiguration("application-errors").onException(IllegalArgumentException.class)
                            .handled(true).to("direct:compensate");
                }
            });
            context.addRoutes(new PlatformDeadLetterRoute());
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    switch (handler) {
                        case "onException" -> onException(IllegalArgumentException.class).handled(true)
                                .to("direct:compensate");
                        case "continued" -> onException(IllegalArgumentException.class).continued(true)
                                .to("direct:compensate");
                        case "not handled" -> onException(IllegalArgumentException.class).handled(false)
                                .to("direct:compensate");
                        default -> { }
                    }
                    var input = from("direct:input").routeId("input")
                            .routeConfigurationId(handler.equals("route configuration onException")
                                    ? PlatformRouteConfiguration.NO_RETRY + ",application-errors"
                                    : PlatformRouteConfiguration.NO_RETRY)
                            .routePolicy(new MessageStoreRoutePolicy(result.rows::add, "order-pipeline"));
                    if (handler.equals("route onException")) {
                        input.onException(IllegalArgumentException.class).handled(true).to("direct:compensate").end();
                    }
                    // The route transforms the input and sets the request headers for a receiver.
                    input.process(e -> {
                        e.getMessage().setBody("<Transformed/>");
                        e.getMessage().setHeader("Authorization", "Bearer route-token");
                        e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-2");
                    });
                    if (handler.equals("doCatch")) {
                        input.doTry().process(rejection())
                                .doCatch(IllegalArgumentException.class).to("direct:compensate").end();
                    } else {
                        input.process(rejection()).to("direct:next");
                    }
                    for (String receiver : List.of("compensate", "next", "parking")) {
                        result.received.put(receiver, new ArrayList<>());
                        from("direct:" + receiver).routeId(receiver).errorHandler(noErrorHandler())
                                .process(e -> result.received.get(receiver).add(Received.of(e)));
                    }
                }
            });
            context.start();
            try (var producer = context.createProducerTemplate()) {
                producer.request("direct:input", e -> {
                    e.getMessage().setBody("<Input/>");
                    e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-1");
                    e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT,
                            (KafkaManualCommit) result.commits::incrementAndGet);
                });
            }
        }
        return result;
    }

    private static Processor rejection() {
        return e -> { throw new IllegalArgumentException("synthetic receiver rejection"); };
    }

    private record Received(String body, Map<String, Object> headers) {
        static Received of(Exchange exchange) {
            return new Received(exchange.getMessage().getBody(String.class),
                    new HashMap<>(exchange.getMessage().getHeaders()));
        }
    }

    private static final class Result {
        final List<MessageAttemptResult> rows = new ArrayList<>();
        final Map<String, List<Received>> received = new HashMap<>();
        final AtomicInteger commits = new AtomicInteger();
    }
}
