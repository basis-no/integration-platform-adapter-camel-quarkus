package no.basis.integrationplatform.camel;

import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.kafka.KafkaEndpoint;
import org.apache.camel.component.kafka.KafkaHeaderFilterStrategy;
import org.apache.camel.processor.SendProcessor;
import org.apache.camel.spi.HeaderFilterStrategy;
import org.apache.camel.spi.InterceptSendToEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Warns when a Kafka send publishes a header whose name marks it as a credential. Camel's
 * Kafka producer publishes every header its header filter strategy lets through, by default
 * all but Camel's own and {@code kafka.*}, so an HTTP intake that keeps the caller's headers
 * publishes the caller's {@code Authorization} with the message. Names follow the DLQ masking
 * rule. Detection only: the record is published as before, and the value is never logged.
 */
public final class PublishedCredentialHeaders {
    private static final Logger LOG = LoggerFactory.getLogger(PublishedCredentialHeaders.class);
    // Header names come from messages, so the number of warnings is bounded.
    private static final int MAX_WARNINGS = 1000;

    private final Set<String> warned = ConcurrentHashMap.newKeySet();

    /** Creates a check that warns once per route and header name. */
    public PublishedCredentialHeaders() {
    }

    /**
     * The header filter a Kafka send publishes through.
     * @param target the send's processor
     * @param endpointUri the send's endpoint URI
     * @return the endpoint's filter for a {@code to}, Camel's default for a {@code toD} with a
     *         literal {@code kafka:} scheme, or null when the send is not a Kafka send
     */
    public static HeaderFilterStrategy kafkaFilter(Processor target, String endpointUri) {
        try {
            if (target instanceof SendProcessor send) {
                Endpoint endpoint = send.getEndpoint();
                if (endpoint instanceof InterceptSendToEndpoint intercepted) {
                    endpoint = intercepted.getOriginalEndpoint();
                }
                if (!(endpoint instanceof KafkaEndpoint kafka)) {
                    return null;
                }
                HeaderFilterStrategy filter = kafka.getConfiguration().getHeaderFilterStrategy();
                return filter != null ? filter : new KafkaHeaderFilterStrategy();
            }
            return isKafka(endpointUri) ? new KafkaHeaderFilterStrategy() : null;
        } catch (RuntimeException e) {
            // The warning is optional; it must never stop a route from being created.
            return null;
        }
    }

    static boolean isKafka(String endpointUri) {
        if (endpointUri == null) {
            return false;
        }
        String uri = endpointUri.trim();
        int colon = uri.indexOf(':');
        return colon > 0 && uri.substring(0, colon).equalsIgnoreCase("kafka");
    }

    /**
     * Logs a warning, once per route and header name, for each credential-like header the send
     * would publish. It never changes the exchange and never throws.
     * @param exchange the exchange about to be sent
     * @param filter the send's header filter
     * @param routeId the route that contains the send
     * @param stepId the send's step id
     */
    public void check(Exchange exchange, HeaderFilterStrategy filter, String routeId, String stepId) {
        try {
            for (String name : List.copyOf(exchange.getMessage().getHeaders().keySet())) {
                if (!PlatformRouteConfiguration.isCredentialHeader(name)
                        || !published(exchange.getMessage().getHeader(name))
                        || filter.applyFilterToCamelHeaders(name, exchange.getMessage().getHeader(name), exchange)
                        || warned.size() >= MAX_WARNINGS
                        || !warned.add(routeId + "\n" + name.toLowerCase(Locale.ROOT))) {
                    continue;
                }
                String printable = printable(name);
                LOG.warn("Kafka send {} in route {} publishes header {}. Its name marks it as a credential, and anyone "
                                + "who can read the topic can read the value. Remove the header before the send, for "
                                + "example with removeHeader(\"{}\"). A route that received the message over HTTP "
                                + "should remove the caller's headers with removeHeaders(\"*\") and then set only the "
                                + "headers the message needs",
                        stepId, routeId, printable, printable);
            }
        } catch (RuntimeException e) {
            // A warning must never fail or change a send.
        }
    }

    /** Value types Camel's default Kafka header serializer writes as they are. */
    private static boolean published(Object value) {
        return value instanceof String || value instanceof byte[] || value instanceof Long
                || value instanceof Integer || value instanceof Double || value instanceof Boolean;
    }

    /** A record's header name is arbitrary text: keep it to one log line. */
    private static String printable(String name) {
        StringBuilder text = new StringBuilder(Math.min(name.length(), 200));
        for (int i = 0; i < name.length() && i < 200; i++) {
            char c = name.charAt(i);
            text.append(c < 0x20 || c == 0x7f ? '?' : c);
        }
        return text.toString();
    }
}
