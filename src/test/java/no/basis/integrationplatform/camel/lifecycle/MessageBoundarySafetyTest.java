package no.basis.integrationplatform.camel.lifecycle;

import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.model.RouteDefinition;
import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class MessageBoundarySafetyTest {
    @Test
    void internalReceiverRequestDoesNotBecomeReplayableBusinessInput() throws Exception {
        List<MessageAttemptResult> outcomes = new ArrayList<>();
        try (var context = new DefaultCamelContext()) {
            context.addRoutePolicyFactory(new MessageStoreRoutePolicyFactory(
                    outcomes::add, "order-pipeline", "postgres-inline", 100_000, 30, true));
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:receiver").routeId("receiver-request")
                            .setBody(constant("response"));
                }
            });
            context.start();
            try (var producer = context.createProducerTemplate()) {
                producer.requestBody("direct:receiver", "<TransformedReceiverRequest/>");
            }
            assertTrue(outcomes.isEmpty(), "An internal exchange must not be advertised as replayable input");
        }
    }

    @Test
    void selectsKafkaInputIncludingResolvedEndpointPlaceholder() throws Exception {
        try (var context = new DefaultCamelContext()) {
            var props = new java.util.Properties();
            props.setProperty("input.uri", "kafka:orders");
            context.getPropertiesComponent().setInitialProperties(props);
            var factory = new MessageStoreRoutePolicyFactory(r -> {}, "order-pipeline", "postgres-inline", 100_000, 30, true);
            assertNotNull(factory.createRoutePolicy(context, "orders", new RouteDefinition().from("kafka:orders")));
            assertNotNull(factory.createRoutePolicy(context, "orders", new RouteDefinition().from("{{input.uri}}")));
            assertNull(factory.createRoutePolicy(context, "internal", new RouteDefinition().from("direct:internal")));
            assertNull(factory.createRoutePolicy(context, "http", new RouteDefinition().from("platform-http:/orders")));
        }
    }
}
