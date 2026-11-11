package no.basis.integrationplatform.camel;

import no.basis.integrationplatform.camel.lifecycle.MessageStoreRoutePolicyFactory;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import org.apache.camel.CamelExecutionException;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.model.RouteDefinition;
import org.apache.camel.spi.RoutePolicy;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The failure mode follows the input type: in one application, with the default
 * {@code basis.message-store.failure-mode=dlq} and replay enabled, a Kafka boundary parks its
 * failed message and commits, and an HTTP audit-only boundary records {@code FAILED}, hands the
 * failure back to its caller and records nothing replayable.
 */
class MixedBoundaryFailureModeTest {
    @Test
    void aKafkaBoundaryParksAndAnHttpBoundaryAnswersItsCallerInOneApplication() throws Exception {
        var rows = new CopyOnWriteArrayList<MessageAttemptResult>();
        var parked = new CopyOnWriteArrayList<String>();
        var httpDeliveries = new AtomicInteger();
        var commits = new AtomicInteger();
        var factory = new MessageStoreRoutePolicyFactory(
                rows::add, "order-pipeline", "postgres-inline", 1_048_576, 30, true, "dlq",
                "auto,receive-order");
        // The policies the factory gives each input type; the routes below stand in for the
        // Kafka consumer and the platform-http endpoint.
        RoutePolicy kafkaPolicy = factory.createRoutePolicy(
                null, "orders-input", new RouteDefinition().from("kafka:bip.orders.in"));
        RoutePolicy httpPolicy = factory.createRoutePolicy(
                null, "receive-order", new RouteDefinition().from("platform-http:/orders"));

        try (var context = new DefaultCamelContext()) {
            var properties = new Properties();
            properties.setProperty("basis.message-store.dlq-uri", "direct:parking");
            context.getPropertiesComponent().setOverrideProperties(properties);
            context.addRoutes(new PlatformRouteConfiguration());
            context.addRoutes(new PlatformDeadLetterRoute());
            context.addRoutes(new RouteBuilder() {
                @Override
                public void configure() {
                    from("direct:orders-input").routeId("orders-input")
                            .routeConfigurationId(PlatformRouteConfiguration.NO_RETRY)
                            .routePolicy(kafkaPolicy)
                            .process(e -> { throw new IllegalStateException("carrier rejected"); });
                    from("direct:receive-order").routeId("receive-order")
                            .routePolicy(httpPolicy)
                            .process(e -> {
                                httpDeliveries.incrementAndGet();
                                throw new IllegalStateException("downstream unavailable");
                            });
                    from("direct:parking").routeId("parking").errorHandler(noErrorHandler())
                            .process(e -> parked.add(e.getMessage().getBody(String.class)));
                }
            });
            context.start();

            try (var producer = context.createProducerTemplate()) {
                producer.send("direct:orders-input", e -> {
                    e.getMessage().setBody("<Shipment/>");
                    e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "shipment-1");
                    e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, (KafkaManualCommit) commits::incrementAndGet);
                });

                CamelExecutionException thrown = assertThrows(CamelExecutionException.class,
                        () -> producer.requestBodyAndHeader("direct:receive-order", "<Order/>",
                                PlatformHeaders.MESSAGE_ID, "order-1"));
                assertEquals("downstream unavailable", thrown.getCause().getMessage(),
                        "The caller gets the route's own failure");
            }
        }

        // Kafka: parked with the consumed input, recorded DLQ, then committed.
        assertEquals(List.of("<Shipment/>"), parked, "Only the Kafka input is parked");
        assertEquals(1, commits.get());
        MessageAttemptResult kafka = row(rows, "shipment-1");
        assertEquals(MessageStatus.DLQ, kafka.status());
        assertTrue(kafka.replayable());

        // HTTP: retried as the default route configuration says, recorded FAILED, not parked,
        // and never replayable, although replay is enabled for the application.
        assertEquals(4, httpDeliveries.get(), "initial delivery plus three automatic redeliveries");
        MessageAttemptResult http = row(rows, "order-1");
        assertEquals(MessageStatus.FAILED, http.status());
        assertEquals("downstream unavailable", http.error());
        assertEquals(3, http.autoRedeliveryCount());
        assertFalse(http.replayable());
        assertEquals("a synchronous HTTP boundary is not replayable", http.replayDisabledReason());
        assertEquals("metadata_only", http.payloadStorage());
    }

    @Test
    void aRealHttpRequestGetsTheErrorAndIsRecordedFailedUnderTheDefaultDlqMode() throws Exception {
        var rows = new CopyOnWriteArrayList<MessageAttemptResult>();
        var parked = new CopyOnWriteArrayList<String>();
        try (var context = new DefaultCamelContext()) {
            var configuration = new org.apache.camel.component.platform.http.vertx.VertxPlatformHttpServerConfiguration();
            configuration.setBindHost("127.0.0.1");
            configuration.setBindPort(0);
            var server = new org.apache.camel.component.platform.http.vertx.VertxPlatformHttpServer(configuration);
            context.addService(server);
            var properties = new Properties();
            properties.setProperty("basis.message-store.dlq-uri", "direct:parking");
            context.getPropertiesComponent().setOverrideProperties(properties);
            // The default failure mode and replay setting, and the boundary selection the docs give.
            context.addRoutePolicyFactory(new MessageStoreRoutePolicyFactory(
                    rows::add, "order-pipeline", "postgres-inline", 1_048_576, 30, true, "dlq", "auto,receive-order"));
            context.addRoutes(new PlatformRouteConfiguration());
            context.addRoutes(new PlatformDeadLetterRoute());
            context.addRoutes(new RouteBuilder() {
                @Override
                public void configure() {
                    from("platform-http:/orders?httpMethodRestrict=POST").routeId("receive-order")
                            .routeConfigurationId(PlatformRouteConfiguration.NO_RETRY)
                            .process(e -> { throw new IllegalStateException("downstream unavailable"); });
                    from("direct:parking").routeId("parking").errorHandler(noErrorHandler())
                            .process(e -> parked.add(e.getMessage().getBody(String.class)));
                }
            });
            context.start();
            int port = server.getServer().actualPort();

            try (var client = java.net.http.HttpClient.newHttpClient()) {
                var response = client.send(
                        java.net.http.HttpRequest.newBuilder(java.net.URI.create("http://127.0.0.1:" + port + "/orders"))
                                .header(PlatformHeaders.MESSAGE_ID, "order-2")
                                .POST(java.net.http.HttpRequest.BodyPublishers.ofString("<Order/>"))
                                .build(),
                        java.net.http.HttpResponse.BodyHandlers.ofString());

                assertEquals(500, response.statusCode(), "The caller gets the failure as an HTTP error");
            }
        }

        assertTrue(parked.isEmpty(), "An HTTP input is never parked");
        MessageAttemptResult http = row(rows, "order-2");
        assertEquals(MessageStatus.FAILED, http.status());
        assertEquals("downstream unavailable", http.error());
        assertFalse(http.replayable());
        assertEquals("metadata_only", http.payloadStorage());
    }

    private static MessageAttemptResult row(List<MessageAttemptResult> rows, String messageId) {
        return rows.stream().filter(r -> messageId.equals(r.messageId())).reduce((a, b) -> b).orElseThrow();
    }
}
