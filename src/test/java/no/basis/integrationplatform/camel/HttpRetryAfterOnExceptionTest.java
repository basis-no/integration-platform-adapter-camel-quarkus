package no.basis.integrationplatform.camel;

import com.sun.net.httpserver.HttpServer;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.http.HttpComponent;
import org.apache.camel.http.base.HttpOperationFailedException;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** An application's own onException maximum of 0 or unlimited, with a receiver that sends Retry-After. */
class HttpRetryAfterOnExceptionTest {
    @Test
    void aMaximumOfZeroLeavesNoDelayForTheHandler() throws Exception {
        AtomicReference<Object> delaySeen = new AtomicReference<>("unset");
        AtomicInteger requests = new AtomicInteger();
        run(2, requests, new CopyOnWriteArrayList<>(), route -> route
                .onException(HttpOperationFailedException.class).maximumRedeliveries(0).handled(true)
                .process(e -> delaySeen.set(e.getIn().getHeader(Exchange.REDELIVERY_DELAY))).end());
        assertEquals(1, requests.get(), "No redelivery");
        assertNull(delaySeen.get(), "The handler gets no delay header");
    }

    @Test
    void anUnlimitedMaximumKeepsHonouringRetryAfter() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        List<Long> arrivals = new CopyOnWriteArrayList<>();
        run(2, requests, arrivals, route -> route
                .onException(HttpOperationFailedException.class).maximumRedeliveries(-1).end());
        assertEquals(3, requests.get(), "Two failures, then success");
        assertTrue(arrivals.get(1) - arrivals.get(0) >= 1_500 && arrivals.get(2) - arrivals.get(1) >= 1_500,
                "Waited for Retry-After 2 s, not the configured 1 s and 2 s: " + arrivals);
    }

    private interface Builder {
        void onException(RouteBuilder route);
    }

    private static void run(int failures, AtomicInteger requests, List<Long> arrivals, Builder exceptions)
            throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", x -> {
            arrivals.add(System.currentTimeMillis());
            x.getRequestBody().readAllBytes();
            boolean fail = requests.incrementAndGet() <= failures;
            if (fail) {
                x.getResponseHeaders().add("Retry-After", "2");
            }
            x.sendResponseHeaders(fail ? 503 : 200, -1);
            x.close();
        });
        server.start();
        try (var context = new DefaultCamelContext()) {
            var properties = new Properties();
            properties.setProperty("basis.message-store.dlq-uri", "direct:parking");
            context.getPropertiesComponent().setOverrideProperties(properties);
            context.addComponent("http", new HttpComponent());
            context.addRoutes(new PlatformRouteConfiguration());
            context.addRoutes(new PlatformDeadLetterRoute());
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    exceptions.onException(this);
                    from("direct:input").routeId("input")
                            .to("http://127.0.0.1:" + server.getAddress().getPort() + "/orders").id("send-order");
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
    }
}
