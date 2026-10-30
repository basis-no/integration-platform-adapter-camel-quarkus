package no.basis.integrationplatform.camel;

import no.basis.integrationplatform.camel.lifecycle.MessageStoreRoutePolicyFactory;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import no.basis.integrationplatform.camel.store.MessageStoreSink;
import org.apache.camel.CamelExecutionException;
import org.apache.camel.Exchange;
import org.apache.camel.spi.RoutePolicy;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.model.RouteDefinition;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PlatformRecordFailedRouteTest {
    @Test
    void retriesAndPropagatesFailureWithoutParkingInDlq() throws Exception {
        AtomicInteger deliveries = new AtomicInteger();
        AtomicReference<MessageAttemptResult> recorded = new AtomicReference<>();
        MessageStoreSink sink = recorded::set;
        MessageStoreRoutePolicyFactory policyFactory = new MessageStoreRoutePolicyFactory(
                sink,
                "adapter-test",
                "postgres-inline",
                1_048_576,
                30,
                false,
                "record-failed",
                "receive-order");
        RoutePolicy policy = policyFactory.createRoutePolicy(
                null,
                "receive-order",
                new RouteDefinition().from("platform-http:/orders"));

        DefaultCamelContext context = new DefaultCamelContext();
        context.addRoutes(new PlatformRouteConfiguration(MessageFailureMode.RECORD_FAILED));
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:receive-order")
                        .routeId("receive-order")
                        .routePolicy(policy)
                        .process(exchange -> {
                            deliveries.incrementAndGet();
                            throw new IllegalStateException("downstream unavailable");
                        });
            }
        });

        try {
            context.start();

            CamelExecutionException thrown = assertThrows(
                    CamelExecutionException.class,
                    () -> context.createProducerTemplate().requestBody("direct:receive-order", "request"));

            Exchange failedExchange = thrown.getExchange();
            assertEquals(4, deliveries.get(), "initial delivery plus three automatic redeliveries");
            assertFalse(Boolean.TRUE.equals(
                    failedExchange.getProperty(PlatformExchangeProperties.DLQ_PARKED, Boolean.class)));
            assertEquals("downstream unavailable", failedExchange.getException().getMessage());

            MessageAttemptResult result = recorded.get();
            assertNotNull(result);
            assertEquals(MessageStatus.FAILED, result.status());
            assertEquals("downstream unavailable", result.error());
            assertTrue(result.autoRedelivered());
            assertEquals(3, result.autoRedeliveryCount());
            assertEquals(3, result.autoRedeliveryMax());
            assertFalse(result.replayable());
        } finally {
            context.stop();
        }
    }
}
