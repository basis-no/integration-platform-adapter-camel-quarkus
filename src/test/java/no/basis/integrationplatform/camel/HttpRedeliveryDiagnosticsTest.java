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
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Retry-then-success through Camel's HTTP producer and the default retry policy. The HTTP
 * client's own retries are off, so every request after the first is a Camel redelivery the
 * adapter records, and a 429 or 503 is timed by its {@code Retry-After}.
 */
class HttpRedeliveryDiagnosticsTest {
    @ParameterizedTest(name = "{0} twice, client retries property {1}: {2} redeliveries")
    @CsvSource({
            "500, , 2",
            "503, , 2",
            "429, , 2",
            "503, true, 2",
            "503, false, 1"})
    void recordsEveryRepeatedRequestAsARedelivery(int failure, String clientRetriesDisabled, int redeliveries)
            throws Exception {
        Result result = run(failure, "POST", null, clientRetriesDisabled);
        assertEquals(3, result.requests().get(), "Two failures, then success");
        assertEquals(1, result.rows().size());
        assertEquals(MessageStatus.COMPLETED, result.rows().getFirst().status());
        assertTrue(result.rows().getFirst().autoRedelivered());
        assertEquals(redeliveries, result.rows().getFirst().autoRedeliveryCount());
        assertEquals(3, result.rows().getFirst().autoRedeliveryMax());
    }

    @ParameterizedTest(name = "{0} {1} with Retry-After {2}")
    @CsvSource({
            "503, POST, seconds",
            "429, POST, seconds",
            "503, GET, seconds",
            "503, POST, date",
            "429, GET, date"})
    void waitsForRetryAfterBeforeTheRedelivery(int failure, String method, String form) throws Exception {
        Result result = run(failure, method, form, null);
        assertEquals(3, result.requests().get());
        assertEquals(2, result.rows().getFirst().autoRedeliveryCount(), "Each repeat is a recorded redelivery");
        // Retry-After asks for 3 s or more each time, more than the configured 1 s and 2 s.
        long first = result.arrivals().get(1) - result.arrivals().get(0);
        long second = result.arrivals().get(2) - result.arrivals().get(1);
        assertTrue(first >= 2_500 && second >= 2_500, "Waited for Retry-After: " + first + " ms, " + second + " ms");
    }

    @ParameterizedTest(name = "{0} {1} with a Retry-After beyond the budget")
    @CsvSource({"503, POST", "429, GET"})
    void aRetryAfterBeyondTheBudgetGetsTheConfiguredDelay(int failure, String method) throws Exception {
        Result result = run(failure, method, "too-long", null);
        assertEquals(3, result.requests().get());
        assertEquals(2, result.rows().getFirst().autoRedeliveryCount());
        long total = result.arrivals().get(2) - result.arrivals().get(0);
        assertTrue(total >= 2_500 && total < 10_000, "The configured 1 s and 2 s, not an hour: " + total + " ms");
    }

    private record Result(AtomicInteger requests, List<Long> arrivals, List<MessageAttemptResult> rows) {
    }

    private static Result run(int failure, String method, String retryAfter, String clientRetriesDisabled)
            throws Exception {
        List<MessageAttemptResult> rows = new ArrayList<>();
        AtomicInteger requests = new AtomicInteger();
        List<Long> arrivals = new CopyOnWriteArrayList<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", x -> {
            arrivals.add(System.currentTimeMillis());
            x.getRequestBody().readAllBytes();
            boolean fail = requests.incrementAndGet() <= 2;
            if (fail && retryAfter != null) {
                x.getResponseHeaders().add("Retry-After", switch (retryAfter) {
                    case "seconds" -> "3";
                    case "date" -> DateTimeFormatter.RFC_1123_DATE_TIME.format(
                            Instant.now().plusSeconds(4).atZone(ZoneOffset.UTC));
                    default -> "3600";
                });
            }
            x.sendResponseHeaders(fail ? failure : 200, -1);
            x.close();
        });
        server.start();
        try (var context = new DefaultCamelContext()) {
            var properties = new Properties();
            properties.setProperty("basis.message-store.dlq-uri", "direct:parking");
            if (clientRetriesDisabled != null) {
                properties.setProperty("camel.component.http.automatic-retries-disabled", clientRetriesDisabled);
            }
            context.getPropertiesComponent().setOverrideProperties(properties);
            var http = new HttpComponent();
            if (clientRetriesDisabled != null) {
                // What Camel's own configuration does with the application's property.
                http.setAutomaticRetriesDisabled(Boolean.parseBoolean(clientRetriesDisabled));
            }
            context.addComponent("http", http);
            context.addRoutes(new PlatformRouteConfiguration());
            context.addRoutes(new PlatformDeadLetterRoute());
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:input").routeId("input")
                            .routePolicy(new MessageStoreRoutePolicy(rows::add, "retry-test"))
                            .setHeader("CamelHttpMethod", constant(method))
                            .to("http://127.0.0.1:" + server.getAddress().getPort() + "/invoices").id("send-invoice");
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
        return new Result(requests, arrivals, rows);
    }
}
