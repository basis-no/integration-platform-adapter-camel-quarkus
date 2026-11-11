package no.basis.integrationplatform.camel;

import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePropertyKey;
import org.apache.camel.component.kafka.KafkaConfiguration;
import org.apache.camel.component.kafka.KafkaEndpoint;
import org.apache.camel.component.kafka.KafkaHeaderFilterStrategy;
import org.apache.camel.component.kafka.serde.DefaultKafkaHeaderSerializer;
import org.apache.camel.spi.HeaderFilterStrategy;
import org.apache.camel.spi.InterceptSendToEndpoint;
import org.apache.camel.support.DefaultHeaderFilterStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Warns when a Kafka send has published a header whose name marks it as a credential.
 * Camel's Kafka producer publishes every header its header filter strategy lets through, by
 * default all but Camel's own and {@code kafka.*}, so an HTTP intake that keeps the caller's
 * headers publishes the caller's {@code Authorization} with the message. Names follow the DLQ
 * masking rule. Detection only: the record is published as before, and the value is never
 * logged. The check runs after a successful send, against the endpoint the send used, and
 * only when that endpoint uses Camel's own header filter strategy, configured with JDK types
 * only, and Camel's own header serializer: the adapter never calls application code to find
 * out what was published.
 */
public final class PublishedCredentialHeaders {
    private static final Logger LOG = LoggerFactory.getLogger(PublishedCredentialHeaders.class);
    // Header names come from messages, so the warnings per route are bounded.
    private static final int MAX_NAMES_PER_ROUTE = 100;

    private final Map<String, RouteNames> warned = new ConcurrentHashMap<>();

    /** Creates a check that warns once per route and header name. */
    public PublishedCredentialHeaders() {
    }

    /**
     * Whether a send may publish to Kafka: an endpoint of Camel's Kafka component, or a
     * {@code toD} whose URI has a literal {@code kafka:} scheme or one chosen at send time.
     * @param endpoint the send's endpoint, or null for a {@code toD}
     * @param endpointUri the send's endpoint URI
     * @param dynamicScheme whether the send chooses its component at send time
     * @return true when the send must be checked after it succeeds
     */
    public static boolean mayPublishToKafka(Endpoint endpoint, String endpointUri, boolean dynamicScheme) {
        try {
            return endpoint != null ? kafkaEndpoint(endpoint) != null : dynamicScheme || isKafka(endpointUri);
        } catch (RuntimeException e) {
            // The warning is optional; it must never stop a route from being created.
            return false;
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
     * Logs a warning, once per route and header name, for each credential-like header the
     * send published. It never changes the exchange and never throws.
     * @param exchange the exchange after a successful send
     * @param endpoint the send's endpoint, or null to use the endpoint a {@code toD} resolved
     * @param routeId the route that contains the send
     * @param stepId the send's step id
     */
    public void check(Exchange exchange, Endpoint endpoint, String routeId, String stepId) {
        try {
            if (endpoint == null) {
                String resolved = exchange.getProperty(ExchangePropertyKey.TO_ENDPOINT, String.class);
                endpoint = resolved == null ? null : exchange.getContext().hasEndpoint(resolved);
            }
            KafkaEndpoint kafka = kafkaEndpoint(endpoint);
            if (kafka == null) {
                return;
            }
            KafkaConfiguration configuration = kafka.getConfiguration();
            HeaderFilterStrategy filter = configuration.getHeaderFilterStrategy();
            if (!camelsOwn(filter) || !camelsOwn(configuration.getHeaderSerializer())) {
                return;
            }
            for (String name : List.copyOf(exchange.getMessage().getHeaders().keySet())) {
                Object value = exchange.getMessage().getHeader(name);
                if (PlatformRouteConfiguration.isCredentialHeader(name) && published(value)
                        && !filter.applyFilterToCamelHeaders(name, value, exchange)) {
                    warnOnce(routeId, stepId, name);
                }
            }
        } catch (RuntimeException e) {
            // A warning must never fail or change a send.
        }
    }

    private void warnOnce(String routeId, String stepId, String name) {
        RouteNames names = warned.computeIfAbsent(String.valueOf(routeId), route -> new RouteNames());
        Admission admission = names.admit(name.toLowerCase(Locale.ROOT));
        if (admission == Admission.LIMIT_REACHED) {
            LOG.warn("Kafka sends in route {} published credential-like headers under {} different names; "
                    + "further names in this route are not logged", routeId, MAX_NAMES_PER_ROUTE);
        }
        if (admission != Admission.NEW) {
            return;
        }
        String printable = printable(name);
        LOG.warn("Kafka send {} in route {} published header {}. Its name marks it as a credential, and anyone "
                        + "who can read the topic can read the value. Remove the header before the send, for "
                        + "example with removeHeader(\"{}\"). A route that received the message over HTTP "
                        + "should remove the caller's headers with removeHeaders(\"*\") and then set only the "
                        + "headers the message needs",
                stepId, routeId, printable, printable);
    }

    private static KafkaEndpoint kafkaEndpoint(Endpoint endpoint) {
        if (endpoint instanceof InterceptSendToEndpoint intercepted) {
            endpoint = intercepted.getOriginalEndpoint();
        }
        return endpoint instanceof KafkaEndpoint kafka ? kafka : null;
    }

    /**
     * Camel's own classes decide from their configuration alone, and the configuration of a
     * filter is checked for an application collection; application code could notice a call.
     * Reading an unset filter set creates an empty one, which filters exactly as none.
     */
    private static boolean camelsOwn(Object strategy) {
        if (strategy instanceof DefaultHeaderFilterStrategy filter
                && (filter.getClass() == KafkaHeaderFilterStrategy.class
                        || filter.getClass() == DefaultHeaderFilterStrategy.class)) {
            return jdkSet(filter.getOutFilter());
        }
        return strategy != null && strategy.getClass() == DefaultKafkaHeaderSerializer.class;
    }

    private static boolean jdkSet(Set<String> set) {
        return set.getClass() == HashSet.class || set.getClass() == LinkedHashSet.class
                || set.getClass().getName().startsWith("java.util.ImmutableCollections$");
    }

    private enum Admission { NEW, KNOWN, LIMIT_REACHED, SUPPRESSED }

    /** The names warned about in one route; admission is atomic, so the bound holds. */
    private static final class RouteNames {
        private final Set<String> names = new HashSet<>();
        private boolean limitReached;

        synchronized Admission admit(String name) {
            if (names.contains(name)) {
                return Admission.KNOWN;
            }
            if (names.size() < MAX_NAMES_PER_ROUTE) {
                names.add(name);
                return Admission.NEW;
            }
            if (limitReached) {
                return Admission.SUPPRESSED;
            }
            limitReached = true;
            return Admission.LIMIT_REACHED;
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
