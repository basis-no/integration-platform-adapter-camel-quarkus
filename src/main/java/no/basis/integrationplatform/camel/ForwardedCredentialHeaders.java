package no.basis.integrationplatform.camel;

import org.apache.camel.Exchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Keeps a consumed Kafka record's credential headers off HTTP sends. Kafka headers
 * arrive as byte arrays, and an HTTP producer writes a byte array as its list of
 * numbers, so a caller's {@code Authorization} that an intake published with the
 * message reached the next receiver in decodable form. A route's own credential
 * header is text and is sent as before.
 */
public final class ForwardedCredentialHeaders {
    private static final Logger LOG = LoggerFactory.getLogger(ForwardedCredentialHeaders.class);
    // Camel's http component writes a byte array header as its list of numbers. Other HTTP
    // producers may convert it to text, which a route can rely on; they are left alone.
    private static final Set<String> HTTP_SCHEMES = Set.of("http", "https");
    private static final Set<String> WARNED = ConcurrentHashMap.newKeySet();

    private ForwardedCredentialHeaders() {
    }

    /**
     * Whether a send goes through Camel's http component.
     * @param endpointUri the send's endpoint URI
     * @return true for {@code http:} and {@code https:} endpoints
     */
    public static boolean isHttp(String endpointUri) {
        if (endpointUri == null) {
            return false;
        }
        int colon = endpointUri.indexOf(':');
        return colon > 0 && HTTP_SCHEMES.contains(endpointUri.substring(0, colon).toLowerCase(Locale.ROOT));
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
                if (WARNED.add(stepId + "\n" + name.toLowerCase(Locale.ROOT))) {
                    LOG.warn("Header {} from the consumed Kafka record was not sent by step {}. A credential "
                            + "received with a message must not reach the next receiver; the publishing route "
                            + "should remove caller headers before it publishes", name, stepId);
                }
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
}
