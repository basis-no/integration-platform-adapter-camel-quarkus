package no.basis.integrationplatform.camel.step;

import org.apache.camel.Exchange;

import java.util.Locale;

/**
 * Runtime input available to a {@link StepClassifier}.
 *
 * @param endpointUri intercepted Camel endpoint URI
 * @param nodeId stable Camel model node id, when present
 * @param exchange current Camel exchange
 */
public record StepClassificationContext(String endpointUri, String nodeId, Exchange exchange) {
    /**
     * Returns the normalized endpoint URI scheme.
     *
     * @return lower-case scheme, or {@code unknown} when no endpoint URI is available
     */
    public String scheme() {
        if (endpointUri == null) {
            return "unknown";
        }
        int separator = endpointUri.indexOf(':');
        return (separator < 0 ? endpointUri : endpointUri.substring(0, separator)).toLowerCase(Locale.ROOT);
    }

    /**
     * Returns the normalized Camel HTTP method header, when present.
     *
     * @return upper-case HTTP method, or {@code null} when absent
     */
    public String httpMethod() {
        String method = exchange.getMessage().getHeader(Exchange.HTTP_METHOD, String.class);
        return method == null ? null : method.trim().toUpperCase(Locale.ROOT);
    }
}
