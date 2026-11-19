package no.basis.integrationplatform.camel.lifecycle;

import no.basis.integrationplatform.camel.ForwardedCredentialHeaders;
import no.basis.integrationplatform.camel.PlatformHeaders;
import org.apache.camel.Exchange;
import org.apache.camel.Route;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An application's own credential-like x-bip- header on a consumed record stays bytes at the
 * boundary, so an HTTP send withholds it like any other credential header of the record.
 */
class CredentialBipHeaderTest {
    @Test
    void aCredentialLikeBipHeaderStaysBytesAndIsWithheldFromAnHttpSend() throws Exception {
        try (var context = new DefaultCamelContext()) {
            Exchange exchange = new DefaultExchange(context);
            exchange.getMessage().setHeader(KafkaConstants.TOPIC, "bip.order-pipeline.in");
            exchange.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, (KafkaManualCommit) () -> { });
            exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-1".getBytes(StandardCharsets.UTF_8));
            exchange.getMessage().setHeader("x-bip-partner-key", "partner-secret-9".getBytes(StandardCharsets.UTF_8));
            exchange.getMessage().setHeader("x-bip-order-ref", "ref-1".getBytes(StandardCharsets.UTF_8));
            exchange.getMessage().setBody("{}");
            new MessageStoreRoutePolicy(result -> { }, "order-pipeline").onExchangeBegin(route(context), exchange);

            assertEquals("message-1", exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID));
            assertEquals("ref-1", exchange.getMessage().getHeader("x-bip-order-ref"), "Other x-bip- headers become text");
            assertInstanceOf(byte[].class, exchange.getMessage().getHeader("x-bip-partner-key"));

            var withheld = ForwardedCredentialHeaders.withhold(exchange, "post-order");
            assertTrue(withheld.containsKey("x-bip-partner-key"));
            assertNull(exchange.getMessage().getHeader("x-bip-partner-key"));
        }
    }

    private static Route route(DefaultCamelContext context) {
        return (Route) Proxy.newProxyInstance(CredentialBipHeaderTest.class.getClassLoader(), new Class<?>[]{Route.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getRouteId", "getId" -> "input";
                    case "getCamelContext" -> context;
                    case "getEndpoint" -> null;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
