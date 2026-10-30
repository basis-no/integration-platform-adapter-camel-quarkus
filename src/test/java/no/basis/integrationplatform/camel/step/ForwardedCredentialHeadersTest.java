package no.basis.integrationplatform.camel.step;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpServer;
import no.basis.integrationplatform.camel.ForwardedCredentialHeaders;
import no.basis.integrationplatform.camel.LogCapture;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An intake that kept the caller's headers published its Authorization with the message.
 * The Kafka route then sent it to its receiver as "[66, 101, ...]", which decodes to the
 * caller's credential.
 */
class ForwardedCredentialHeadersTest {
    private static final byte[] CALLER_AUTHORIZATION = "Bearer synthetic-caller".getBytes(StandardCharsets.UTF_8);
    private static final byte[] CALLER_KEY = "synthetic-key".getBytes(StandardCharsets.UTF_8);

    @Test
    void httpSendWithholdsCredentialBytesFromTheRecordAndKeepsTheRoutesOwn() throws Exception {
        List<Headers> received = new ArrayList<>();
        List<Object> after = new ArrayList<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", x -> {
            received.add(x.getRequestHeaders());
            x.sendResponseHeaders(200, -1);
            x.close();
        });
        server.start();
        try (var logs = LogCapture.of(ForwardedCredentialHeaders.class);
             var context = new DefaultCamelContext()) {
            context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(new NoStepTracker()));
            String receiver = "http://127.0.0.1:" + server.getAddress().getPort();
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:record").routeId("record")
                            .to(receiver + "/forwarded").id("post-forwarded")
                            .process(e -> {
                                after.add(e.getMessage().getHeader("Authorization"));
                                after.add(e.getMessage().getHeader("X-Api-Key"));
                            })
                            .setHeader("Authorization", constant("Basic route-own"))
                            .to(receiver + "/own").id("post-own");
                }
            });
            context.start();
            try (var producer = context.createProducerTemplate()) {
                producer.send("direct:record", e -> {
                    e.getMessage().setHeader("Authorization", CALLER_AUTHORIZATION);
                    e.getMessage().setHeader("X-Api-Key", CALLER_KEY);
                    e.getMessage().setHeader("X-Trace-Note", "trace".getBytes(StandardCharsets.UTF_8));
                    e.getMessage().setBody("{}");
                });
                producer.send("direct:record", e -> {
                    e.getMessage().setHeader("Authorization", CALLER_AUTHORIZATION);
                    e.getMessage().setBody("{}");
                });
            }
            assertEquals(4, received.size());
            assertNull(received.get(0).getFirst("Authorization"));
            assertNull(received.get(0).getFirst("X-Api-Key"));
            assertTrue(received.get(0).containsKey("X-Trace-Note"), "Other headers are sent as before");
            assertEquals("Basic route-own", received.get(1).getFirst("Authorization"));
            assertArrayEquals(CALLER_AUTHORIZATION, (byte[]) after.get(0),
                    "The route still sees the record's header after the send");
            assertArrayEquals(CALLER_KEY, (byte[]) after.get(1));
            assertNull(received.get(2).getFirst("Authorization"));

            var warnings = logs.at(Level.WARNING);
            assertEquals(3, warnings.size(), "One warning per step and header name: two on the first send, the key on the second");
            assertTrue(warnings.stream().noneMatch(r -> LogCapture.text(r).contains("synthetic")), "The value is never logged");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void coversCamelsHttpComponentOnly() {
        assertTrue(ForwardedCredentialHeaders.isHttp("https://receiver.example/orders"));
        assertTrue(ForwardedCredentialHeaders.isHttp("HTTP://receiver.example/orders"));
        assertFalse(ForwardedCredentialHeaders.isHttp("vertx-http:https://receiver.example"),
                "Other HTTP producers may send the bytes as text, which a route can rely on");
        assertFalse(ForwardedCredentialHeaders.isHttp("kafka:bip.orders.in"));
        assertFalse(ForwardedCredentialHeaders.isHttp("sql:select 1"));
        assertFalse(ForwardedCredentialHeaders.isHttp(null));
    }

    private static final class NoStepTracker implements StepTracker {
        public void beforeSend(Exchange e) {}
        public void recordDelivered(Exchange e) {}
        public void recordSkipped(Exchange e) {}
        public void recordFailed(Exchange e) {}
        public void recordFailedAndRethrow(Exchange e) {}
    }
}
