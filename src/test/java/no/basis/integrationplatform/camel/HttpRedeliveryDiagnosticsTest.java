package no.basis.integrationplatform.camel;

import com.sun.net.httpserver.HttpServer;
import no.basis.integrationplatform.camel.lifecycle.MessageStoreRoutePolicy;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.http.HttpComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Retry-then-success through Camel's HTTP producer and the default retry policy. The
 * count is Camel's redeliveries. The HTTP client retries a 503 or 429 once by itself
 * within one Camel attempt, so the receiver sees more requests than the count says.
 */
class HttpRedeliveryDiagnosticsTest {
    @ParameterizedTest(name = "{0} twice, client retries disabled {1}: {2} redeliveries")
    @CsvSource({
            "500, false, 2",
            "503, false, 1",
            "429, false, 1",
            "503, true, 2"})
    void recordsCamelRedeliveriesWhenARetrySucceeds(int failure, boolean clientRetriesDisabled, int redeliveries)
            throws Exception {
        List<MessageAttemptResult> rows = new ArrayList<>();
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", x -> {
            x.getRequestBody().readAllBytes();
            x.sendResponseHeaders(requests.incrementAndGet() <= 2 ? failure : 200, -1);
            x.close();
        });
        server.start();
        try (var context = new DefaultCamelContext()) {
            var properties = new Properties();
            properties.setProperty("basis.message-store.dlq-uri", "direct:parking");
            context.getPropertiesComponent().setOverrideProperties(properties);
            var http = new HttpComponent();
            http.setAutomaticRetriesDisabled(clientRetriesDisabled);
            context.addComponent("http", http);
            context.addRoutes(new PlatformRouteConfiguration());
            context.addRoutes(new PlatformDeadLetterRoute());
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:input").routeId("input")
                            .routePolicy(new MessageStoreRoutePolicy(rows::add, "retry-test"))
                            .to("http://127.0.0.1:" + server.getAddress().getPort() + "/invoices").id("post-invoice");
                    from("direct:parking").errorHandler(noErrorHandler()).log("parked");
                }
            });
            context.start();
            try (var producer = context.createProducerTemplate()) {
                producer.request("direct:input", e -> e.getMessage().setBody("{}"));
            }
        } finally {
            server.stop(0);
        }
        assertEquals(3, requests.get(), "Two failures, then success");
        assertEquals(1, rows.size());
        assertEquals(MessageStatus.COMPLETED, rows.getFirst().status());
        assertTrue(rows.getFirst().autoRedelivered());
        assertEquals(redeliveries, rows.getFirst().autoRedeliveryCount());
        assertEquals(3, rows.getFirst().autoRedeliveryMax());
    }
}
