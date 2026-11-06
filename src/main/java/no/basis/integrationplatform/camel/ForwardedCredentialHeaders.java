package no.basis.integrationplatform.camel;

import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.spi.EndpointStrategy;
import org.apache.camel.spi.HeaderFilterStrategy;
import org.apache.camel.spi.HeaderFilterStrategyAware;
import org.apache.camel.spi.InterceptSendToEndpoint;
import org.apache.camel.spi.PropertyConfigurer;
import org.apache.camel.spi.PropertyConfigurerGetter;
import org.apache.camel.util.URISupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Keeps a consumed Kafka record's credential headers, and Camel's Kafka consumer headers, off
 * HTTP sends. Kafka headers arrive as byte arrays, and an HTTP producer sends a byte array as
 * its list of numbers or as text, so a caller's {@code Authorization} that an intake published
 * with the message reached the next receiver in decodable form. A credential header the route
 * sets itself is text and is sent as before. Camel's Kafka consumer also sets headers named
 * {@code kafka.*} on camel-kafka 4.0.0 to 4.14.7, 4.15.0 to 4.18.2 and 4.19.0 to 4.20.0, and
 * HTTP producers send them: {@code kafka.HEADERS} lists the bytes of every record header,
 * credentials included. Later versions name them {@code CamelKafka*}, which Camel's default
 * HTTP header filters drop; an application's own filter may let them through.
 * <p>
 * A tracked {@code to} or {@code toD} through Camel's http component withholds the credential
 * headers around the send. {@link #install} adds both kinds to the header filter of every
 * {@code http}, {@code https}, {@code netty-http} and {@code vertx-http} endpoint the context
 * registers, so the producer leaves them out of the request whichever EIP sends. The exchange
 * keeps the headers. An endpoint whose filter is unset, which then sends no headers, is left
 * alone. Not covered: an endpoint Camel does not register, such as one a {@code toD} with
 * {@code cacheSize=-1} creates for a single send, and a filter replaced after the endpoint was
 * registered, as the {@code rest:} producer does for the endpoint it sends through.
 */
public final class ForwardedCredentialHeaders {
    private static final Logger LOG = LoggerFactory.getLogger(ForwardedCredentialHeaders.class);
    private static final Set<String> HTTP_SCHEMES = Set.of("http", "https");
    private static final Set<String> HTTP_COMPONENTS = Set.of("http", "https", "netty-http", "vertx-http");
    // Camel's Kafka headers under both names: an application's own filter may let Camel*
    // headers through.
    private static final List<String> KAFKA_PREFIXES = List.of("kafka.", "CamelKafka");
    private static final String HEADER_FILTER_STRATEGY = "headerFilterStrategy";
    private static final Set<String> WARNED = ConcurrentHashMap.newKeySet();
    private static final Map<CamelContext, FilterHttpEndpoints> INSTALLED = new WeakHashMap<>();

    private ForwardedCredentialHeaders() {
    }

    /**
     * Whether a send goes through Camel's http component.
     * @param endpointUri the send's endpoint URI
     * @return true for {@code http:} and {@code https:} endpoints
     */
    public static boolean isHttp(String endpointUri) {
        return hasScheme(endpointUri, HTTP_SCHEMES);
    }

    private static boolean hasScheme(String endpointUri, Set<String> schemes) {
        if (endpointUri == null) {
            return false;
        }
        // Camel trims a resolved endpoint URI before it picks the component.
        String uri = endpointUri.trim();
        int colon = uri.indexOf(':');
        return colon > 0 && schemes.contains(uri.substring(0, colon).toLowerCase(Locale.ROOT));
    }

    /**
     * Removes credential-like headers that still hold raw Kafka bytes.
     * @param exchange the exchange about to be sent
     * @param stepId the send's step id, for the warning
     * @return the removed headers, to restore after the send
     */
    public static Map<String, Object> withhold(Exchange exchange, String stepId) {
        Map<String, Object> withheld = null;
        for (String name : List.copyOf(exchange.getMessage().getHeaders().keySet())) {
            Object value = exchange.getMessage().getHeader(name);
            if (value instanceof byte[] && PlatformRouteConfiguration.isCredentialHeader(name)) {
                if (withheld == null) {
                    withheld = new LinkedHashMap<>();
                }
                withheld.put(name, exchange.getMessage().removeHeader(name));
                warn(name, "step " + stepId);
            }
        }
        return withheld == null ? Map.of() : withheld;
    }

    /**
     * Puts withheld headers back after the send, unless the response set the same name.
     * @param exchange the exchange after the send
     * @param withheld the headers {@link #withhold} removed
     */
    public static void restore(Exchange exchange, Map<String, Object> withheld) {
        withheld.forEach((name, value) -> {
            if (exchange.getMessage().getHeader(name) == null) {
                exchange.getMessage().setHeader(name, value);
            }
        });
    }

    /**
     * Keeps a consumed record's credential headers and Camel's {@code kafka.*} consumer headers
     * out of every request of the HTTP endpoints the context registers. Installs once per
     * context and also covers the endpoints already registered.
     * @param context the Camel context
     */
    public static void install(CamelContext context) {
        synchronized (INSTALLED) {
            FilterHttpEndpoints filter = INSTALLED.get(context);
            if (filter != null && filter.caughtUp) {
                return;
            }
            if (filter == null) {
                filter = new FilterHttpEndpoints();
                INSTALLED.put(context, filter);
                // Camel keeps the callback, and runs it on the registered endpoints once.
                context.getCamelContextExtension().registerEndpointCallback(filter);
            }
            // Again, until it completes once: an endpoint that cannot be extended stops the
            // catch-up, and the endpoints after it must not stay unfiltered when the
            // application starts again without it.
            for (Endpoint endpoint : List.copyOf(context.getEndpoints())) {
                filter.registerEndpoint(endpoint.getEndpointUri(), endpoint);
            }
            filter.caughtUp = true;
        }
    }

    private static void warn(String name, String sender) {
        if (WARNED.add(sender + "\n" + name.toLowerCase(Locale.ROOT))) {
            LOG.warn("Header {} from the consumed Kafka record was not sent by {}. A credential "
                    + "received with a message must not reach the next receiver; the publishing route "
                    + "should remove caller headers before it publishes", name, sender);
        }
    }

    /**
     * Adds the record headers to the header filter of each HTTP endpoint as it is registered,
     * keeping the endpoint's own filter. An endpoint without a filter sends no headers and is
     * left alone. An HTTP endpoint whose filter cannot be read or replaced fails, so the route
     * does not start without it.
     */
    private static final class FilterHttpEndpoints implements EndpointStrategy {
        // No reference to the context, which is the weak key of INSTALLED.
        private volatile boolean caughtUp;

        @Override
        public Endpoint registerEndpoint(String uri, Endpoint registered) {
            // interceptSendToEndpoint registers a wrapper around the endpoint that sends.
            Endpoint endpoint = registered;
            while (endpoint instanceof InterceptSendToEndpoint intercepted && intercepted.getOriginalEndpoint() != endpoint) {
                endpoint = intercepted.getOriginalEndpoint();
            }
            if (endpoint == null || !hasScheme(endpoint.getEndpointUri(), HTTP_COMPONENTS)) {
                return registered;
            }
            PropertyConfigurer configurer = endpoint.getComponent() == null ? null
                    : endpoint.getComponent().getEndpointPropertyConfigurer();
            if (configurer instanceof PropertyConfigurerGetter getter
                    && getter.getOptionType(HEADER_FILTER_STRATEGY, true) instanceof Class<?> type
                    && HeaderFilterStrategy.class.isAssignableFrom(type)) {
                // The component's own option: camel-http's endpoint, vertx-http's configuration,
                // netty-http's endpoint and binding.
                Object filter = getter.getOptionValue(endpoint, HEADER_FILTER_STRATEGY, true);
                if (filter == null || filter instanceof RecordHeaderFilter) {
                    return registered;
                }
                if (!(filter instanceof HeaderFilterStrategy strategy)
                        || !configurer.configure(endpoint.getCamelContext(), endpoint, HEADER_FILTER_STRATEGY,
                                new RecordHeaderFilter(strategy), true)
                        || !(getter.getOptionValue(endpoint, HEADER_FILTER_STRATEGY, true) instanceof RecordHeaderFilter)) {
                    throw unfiltered(endpoint);
                }
                return registered;
            }
            // A configurer that does not know the option, for example of a subclassed component.
            if (endpoint instanceof HeaderFilterStrategyAware aware) {
                HeaderFilterStrategy filter = aware.getHeaderFilterStrategy();
                if (filter == null || filter instanceof RecordHeaderFilter) {
                    return registered;
                }
                aware.setHeaderFilterStrategy(new RecordHeaderFilter(filter));
                if (aware.getHeaderFilterStrategy() instanceof RecordHeaderFilter) {
                    return registered;
                }
            }
            throw unfiltered(endpoint);
        }

        private static IllegalStateException unfiltered(Endpoint endpoint) {
            return new IllegalStateException("Cannot add a consumed Kafka record's credential headers and the Kafka "
                    + "consumer's kafka.* headers to the header filter of " + URISupport.sanitizeUri(endpoint.getEndpointUri())
                    + "; the endpoint could send them to its receiver");
        }
    }

    /** The endpoint's own filter, which also leaves out the consumed record's headers. */
    private record RecordHeaderFilter(HeaderFilterStrategy filter) implements HeaderFilterStrategy {
        @Override
        public boolean applyFilterToCamelHeaders(String name, Object value, Exchange exchange) {
            if (name != null && KAFKA_PREFIXES.stream().anyMatch(prefix -> name.regionMatches(true, 0, prefix, 0, prefix.length()))) {
                return true;
            }
            if (name != null && PlatformRouteConfiguration.isCredentialHeader(name) && (value instanceof byte[]
                    || exchange != null && exchange.getMessage().getHeader(name) instanceof byte[])) {
                warn(name, "a send in route " + (exchange == null ? null : exchange.getFromRouteId()));
                return true;
            }
            return filter.applyFilterToCamelHeaders(name, value, exchange);
        }

        @Override
        public boolean applyFilterToExternalHeaders(String name, Object value, Exchange exchange) {
            return filter.applyFilterToExternalHeaders(name, value, exchange);
        }
    }
}
