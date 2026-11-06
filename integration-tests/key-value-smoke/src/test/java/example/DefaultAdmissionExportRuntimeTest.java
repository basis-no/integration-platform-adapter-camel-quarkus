package example;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import no.basis.integrationplatform.camel.traffic.TrafficSignals;
import org.apache.camel.CamelContext;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Without any activity configuration, an application without a Kafka boundary exports no
 * activity metrics, and the first Kafka boundary starts the admission counter.
 */
@QuarkusTest
@QuarkusTestResource(KeyValuePostgresResource.class)
class DefaultAdmissionExportRuntimeTest {
    @Inject TrafficSignals signals;
    @Inject CamelContext camel;
    @TestHTTPResource("/q/metrics") URI metrics;

    @Test
    void countsAdmissionByDefaultOnceThereIsAKafkaBoundary() throws Exception {
        assertFalse(scrape().contains("integration_traffic_"), "No Kafka boundary, no activity metrics");

        // As the route policy factory does for a selected Kafka boundary route.
        signals.observeKafkaBoundary();
        var exchange = new DefaultExchange(camel);
        exchange.getMessage().setHeader(KafkaConstants.TOPIC, "example-input");
        exchange.getMessage().setHeader(KafkaConstants.PARTITION, 0);
        exchange.getMessage().setHeader(KafkaConstants.OFFSET, 7L);
        signals.recordAdmitted(exchange);

        String body = scrape();
        assertEquals(1, sample(body, "integration_traffic_events_total", "record_admitted"));
        assertEquals(1, sample(body, "integration_traffic_signal_supported", "record_admitted"));
        assertEquals(0, sample(body, "integration_traffic_signal_supported", "record_published"));
    }

    private String scrape() throws Exception {
        assertTrue(metrics.getHost().equals("localhost") || metrics.getHost().equals("127.0.0.1")
                || metrics.getHost().equals("[::1]"), "HTTP test target must remain loopback");
        try (var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build()) {
            var response = client.send(HttpRequest.newBuilder(metrics).timeout(Duration.ofSeconds(2)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode());
            return response.body();
        }
    }

    private double sample(String body, String metric, String signal) {
        var pattern = Pattern.compile("(?m)^" + Pattern.quote(metric)
                + "\\{[^}]*signal=\"" + Pattern.quote(signal) + "\"[^}]*\\} (\\S+)");
        var match = pattern.matcher(body);
        assertTrue(match.find(), metric + " " + signal + " missing from export:\n" + body);
        return Double.parseDouble(match.group(1));
    }
}
