package no.basis.integrationplatform.camel.lifecycle;

import no.basis.integrationplatform.camel.LogCapture;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import org.apache.camel.Exchange;
import org.apache.camel.Route;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An attempt or schema version that is not a whole number must not make a processed message
 * disappear: the message is recorded with 1 and the header named in a warning.
 */
class EnvelopeNumbersTest {
    private final DefaultCamelContext context = new DefaultCamelContext();
    private final List<MessageAttemptResult> rows = new ArrayList<>();
    private final AtomicInteger commits = new AtomicInteger();

    @AfterEach
    void close() throws Exception {
        context.close();
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "abc|1|1", "2147483648|1|1", "' 3 '|3|3", "+3|3|3", "-2|1|-2", "٣|1|1", "''|1|1", "0x10|1|1"})
    void aRecordWithAnUnreadableNumberIsRecordedAndCommitted(String value, int attempt, int schemaVersion) {
        try (var log = LogCapture.of(MessageStoreRoutePolicy.class)) {
            MessageStoreRoutePolicy policy = new MessageStoreRoutePolicy(rows::add, "order-pipeline");
            Exchange exchange = record("message-1\r\nforged");
            exchange.getMessage().setHeader(PlatformHeaders.ATTEMPT, value.getBytes(StandardCharsets.UTF_8));
            exchange.getMessage().setHeader(PlatformHeaders.SCHEMA_VERSION, value.getBytes(StandardCharsets.UTF_8));
            policy.onExchangeBegin(route(), exchange);
            policy.onExchangeDone(route(), exchange);

            assertEquals(1, rows.size(), "The message is recorded");
            assertEquals(attempt, rows.getFirst().attempt());
            assertEquals(schemaVersion, rows.getFirst().schemaVersion());
            assertEquals(1, commits.get());
            assertNull(exchange.getException());
            List<String> warnings = log.at(Level.WARNING).stream().map(LogCapture::text).toList();
            boolean readable = value.trim().matches("[+-]?[0-9]+") && !value.trim().equals("2147483648");
            assertEquals(readable ? 0 : 2, warnings.size(), warnings.toString());
            for (String warning : warnings) {
                assertTrue(warning.contains("message-1\\u000d\\u000aforged"), "The id is escaped: " + warning);
            }
        }
    }

    @Test
    void anUnexpectedFailureWhileBuildingTheOutcomeKeepsTheRecordUncommitted() {
        MessageStoreRoutePolicy policy = new MessageStoreRoutePolicy(rows::add, "order-pipeline");
        Exchange exchange = record("message-1");
        policy.onExchangeBegin(route(), exchange);
        // A route that leaves a body or header the outcome cannot be built from.
        exchange.getMessage().setHeader(PlatformHeaders.CONTENT_TYPE, new Object() {
            @Override public String toString() { throw new IllegalStateException("unreadable"); }
        });

        try {
            policy.onExchangeDone(route(), exchange);
        } catch (RuntimeException expected) {
            // Camel logs and ignores what a route policy throws; the exchange keeps the failure.
        }

        assertEquals(0, commits.get(), "Not committed, so the record is read again");
        assertTrue(exchange.getException() != null, "The consumer sees a failed exchange");
    }

    private Exchange record(String messageId) {
        Exchange exchange = new DefaultExchange(context);
        exchange.getMessage().setHeader(KafkaConstants.TOPIC, "bip.order-pipeline.in");
        exchange.getMessage().setHeader(KafkaConstants.PARTITION, 0);
        exchange.getMessage().setHeader(KafkaConstants.OFFSET, 7L);
        exchange.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, (KafkaManualCommit) commits::incrementAndGet);
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, messageId.getBytes(StandardCharsets.UTF_8));
        exchange.getMessage().setBody("{}");
        return exchange;
    }

    private Route route() {
        return (Route) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Route.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getRouteId", "getId" -> "input";
                    case "getEndpoint" -> null;
                    case "getCamelContext" -> context;
                    case "toString" -> "route(input)";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
