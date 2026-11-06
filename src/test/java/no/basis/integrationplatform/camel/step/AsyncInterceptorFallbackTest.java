package no.basis.integrationplatform.camel.step;

import no.basis.integrationplatform.camel.LogCapture;
import no.basis.integrationplatform.camel.PlatformDeadLetterRoute;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.impl.engine.DefaultChannel;
import org.apache.camel.support.PluginHelper;
import org.apache.camel.support.ResourceHelper;
import org.apache.camel.support.processor.PredicateValidationException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Camel warns at route start for every processor an intercept strategy hands back without
 * an {@code AsyncProcessor}, and then bridges it itself. The adapter wraps only endpoint sends,
 * so every other processor, such as {@code validate}, used to log that warning.
 */
class AsyncInterceptorFallbackTest {
    private static final String WARNING = "does not return an AsyncProcessor instance";

    @Test
    void javaRoutesStartWithoutTheAsyncFallbackWarningAndKeepTheirBehaviour() throws Exception {
        List<String> tracked = new ArrayList<>();
        try (var logs = LogCapture.of(DefaultChannel.class);
             var context = new DefaultCamelContext()) {
            context.getCamelContextExtension().addInterceptStrategy(
                    new StepTrackingInterceptStrategy(new RecordingStepTracker(tracked)));
            context.addRoutes(new RouteBuilder() {
                @Override
                public void configure() {
                    from("direct:java").routeId("java")
                            .validate(header("valid").isEqualTo(true))
                            .process(e -> e.getMessage().setHeader("processed", true))
                            .filter(header("processed").isEqualTo(true))
                                .setBody(simple("${body}-filtered"))
                            .end()
                            .choice()
                                .when(header("fail").isEqualTo(true))
                                    .process(e -> { throw new IllegalStateException("synthetic processor failure"); })
                                .otherwise()
                                    .log("passed")
                            .end()
                            .to("direct:receiver").id("deliver-order");
                    from("direct:receiver").routeId("receiver").setBody(simple("${body}-received"));
                    from("direct:parking").routeId(PlatformDeadLetterRoute.ROUTE_ID)
                            .validate(body().isNotNull())
                            .setBody(simple("${body}-parked"));
                }
            });
            context.start();

            assertNoAsyncFallbackWarning(logs);
            try (var producer = context.createProducerTemplate()) {
                Exchange passed = producer.request("direct:java", e -> {
                    e.getMessage().setHeader("valid", true);
                    e.getMessage().setBody("order");
                });
                assertNull(passed.getException());
                assertEquals("order-filtered-received", passed.getMessage().getBody());
                assertEquals(List.of("before", "delivered"), tracked, "Only the endpoint send is tracked");

                Exchange invalid = producer.request("direct:java", e -> e.getMessage().setHeader("valid", false));
                assertInstanceOf(PredicateValidationException.class, invalid.getException());

                Exchange failed = producer.request("direct:java", e -> {
                    e.getMessage().setHeader("valid", true);
                    e.getMessage().setHeader("fail", true);
                });
                assertInstanceOf(IllegalStateException.class, failed.getException());
                assertEquals("synthetic processor failure", failed.getException().getMessage());

                assertEquals("order-parked", producer.requestBody("direct:parking", "order"));
            }
        }
    }

    @Test
    void yamlRoutesStartWithoutTheAsyncFallbackWarning() throws Exception {
        String yaml = """
                - route:
                    id: yaml
                    from:
                      uri: direct:yaml
                      steps:
                        - validate:
                            simple: "${header.valid} == true"
                        - setBody:
                            simple: "${body}-checked"
                        - to:
                            id: deliver-yaml-order
                            uri: direct:yaml-receiver
                - route:
                    id: yaml-receiver
                    from:
                      uri: direct:yaml-receiver
                      steps:
                        - log:
                            message: received
                """;
        List<String> tracked = new ArrayList<>();
        try (var logs = LogCapture.of(DefaultChannel.class);
             var context = new DefaultCamelContext()) {
            context.getCamelContextExtension().addInterceptStrategy(
                    new StepTrackingInterceptStrategy(new RecordingStepTracker(tracked)));
            PluginHelper.getRoutesLoader(context).loadRoutes(ResourceHelper.fromString("routes.yaml", yaml));
            context.start();

            assertNoAsyncFallbackWarning(logs);
            try (var producer = context.createProducerTemplate()) {
                Exchange passed = producer.request("direct:yaml", e -> {
                    e.getMessage().setHeader("valid", true);
                    e.getMessage().setBody("order");
                });
                assertNull(passed.getException());
                assertEquals("order-checked", passed.getMessage().getBody());
                assertEquals(List.of("before", "delivered"), tracked, "Only the endpoint send is tracked");
                Exchange invalid = producer.request("direct:yaml", e -> e.getMessage().setHeader("valid", false));
                assertInstanceOf(PredicateValidationException.class, invalid.getException());
            }
        }
    }

    private static void assertNoAsyncFallbackWarning(LogCapture logs) {
        var warnings = logs.at(Level.WARNING).stream().map(LogCapture::text).filter(text -> text.contains(WARNING)).toList();
        assertTrue(warnings.isEmpty(), warnings::toString);
    }

    private record RecordingStepTracker(List<String> calls) implements StepTracker {
        @Override
        public void beforeSend(Exchange e) {
            calls.add("before");
        }

        @Override
        public void recordDelivered(Exchange e) {
            calls.add("delivered");
        }

        @Override
        public void recordSkipped(Exchange e) {
            calls.add("skipped");
        }

        @Override
        public void recordFailed(Exchange e) {
            calls.add("failed");
        }

        @Override
        public void recordFailedAndRethrow(Exchange e) {
            calls.add("failed");
        }
    }
}
