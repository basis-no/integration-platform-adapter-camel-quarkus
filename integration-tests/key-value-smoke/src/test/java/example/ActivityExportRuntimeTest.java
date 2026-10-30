package example;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import no.basis.integrationplatform.camel.traffic.TrafficSignals;
import org.apache.camel.CamelContext;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.KafkaEndpoint;
import org.apache.camel.support.DefaultExchange;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

/** Checks actual CDI/Micrometer HTTP export with synthetic events and no broker. */
@QuarkusTest
@QuarkusTestResource(KeyValuePostgresResource.class)
class ActivityExportRuntimeTest {
    @Inject TrafficSignals signals;
    @Inject CamelContext camel;
    @TestHTTPResource("/q/metrics") URI metrics;

    @Test
    void exportsExactNamesSupportedSamplesAndProcessHint() throws Exception {
        signals.observeKafkaBoundary();
        var exchange = new DefaultExchange(camel);
        exchange.getMessage().setHeader(KafkaConstants.TOPIC, "example-input");
        exchange.getMessage().setHeader(KafkaConstants.PARTITION, 0);
        exchange.getMessage().setHeader(KafkaConstants.OFFSET, 7L);
        signals.recordAdmitted(exchange);
        // Configuration is resolved, but this endpoint is never started or sent.
        var endpoint = camel.getEndpoint("kafka:example-input?brokers=localhost:1"
                + "&recordMetadata=true&requestRequiredAcks=all", KafkaEndpoint.class);
        signals.observeProducerEndpoint(endpoint);
        var pending = signals.beforePublish(endpoint.getEndpointUri(), exchange);
        String before = scrape();
        assertEquals(1, sample(before, "integration_traffic_events_total", "record_admitted"));
        assertEquals(0, sample(before, "integration_traffic_events_total", "record_published"));
        exchange.getMessage().setHeader(KafkaConstants.KAFKA_RECORD_META,
                List.of(new RecordMetadata(new TopicPartition("example-input", 0), 7L, 0, 0L, 0, 0)));
        signals.afterPublish(pending, exchange, true);
        String after = scrape();
        assertEquals(1, sample(after, "integration_traffic_events_total", "record_published"));
        assertEquals(1, sample(after, "integration_traffic_signal_supported", "record_admitted"));
        assertEquals(1, sample(after, "integration_traffic_signal_supported", "record_published"));
        var process = Pattern.compile("(?m)^integration_traffic_process_start_time_seconds (\\S+)").matcher(after);
        assertTrue(process.find(), after);
        assertTrue(Double.parseDouble(process.group(1)) > 0);
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
