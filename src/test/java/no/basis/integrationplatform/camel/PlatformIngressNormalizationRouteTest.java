package no.basis.integrationplatform.camel;

import org.apache.camel.CamelExecutionException;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PlatformIngressNormalizationRouteTest {
    @Test
    void retriesAndReturnsFailureToSourceInsteadOfParkingInPlatformDlq() throws Exception {
        AtomicInteger deliveries = new AtomicInteger();
        DefaultCamelContext context = new DefaultCamelContext();
        context.addRoutes(new PlatformRouteConfiguration(MessageFailureMode.DLQ));
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:source")
                        .routeId("source-to-kafka")
                        .routeConfigurationId(PlatformRouteConfiguration.INGRESS_NORMALIZATION)
                        .process(exchange -> {
                            deliveries.incrementAndGet();
                            throw new IllegalStateException("Kafka publish unavailable");
                        });
            }
        });

        try {
            context.start();

            CamelExecutionException failure = assertThrows(
                    CamelExecutionException.class,
                    () -> context.createProducerTemplate().requestBody("direct:source", "payload"));

            assertEquals(4, deliveries.get(), "initial delivery plus three in-process retries");
            assertEquals("Kafka publish unavailable", failure.getExchange().getException().getMessage());
            assertFalse(Boolean.TRUE.equals(
                    failure.getExchange().getProperty(PlatformExchangeProperties.DLQ_PARKED, Boolean.class)));
        } finally {
            context.stop();
        }
    }
}
