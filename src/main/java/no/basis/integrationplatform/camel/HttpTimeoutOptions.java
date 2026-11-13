package no.basis.integrationplatform.camel;

import org.apache.camel.CamelContext;
import org.apache.camel.NamedNode;
import org.apache.camel.Processor;
import org.apache.camel.model.EnrichDefinition;
import org.apache.camel.model.RouteDefinition;
import org.apache.camel.model.SendDefinition;
import org.apache.camel.model.ToDynamicDefinition;
import org.apache.camel.model.WireTapDefinition;
import org.apache.camel.model.language.ConstantExpression;
import org.apache.camel.model.language.SimpleExpression;
import org.apache.camel.spi.IdAware;
import org.apache.camel.support.EndpointHelper;
import org.apache.camel.support.LanguageSupport;
import org.apache.camel.util.URISupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Warns when a route is created with an HTTP timeout option that Camel 4's {@code http}
 * component accepts without applying it, so a call waits for the component's timeout,
 * three minutes unless configured. Checked on Camel 4.18 and 4.22:
 * <ul>
 * <li>{@code socketTimeout}, a Camel 3 option, is sent to the receiver as a query parameter;</li>
 * <li>{@code httpConnection.*} applies only {@code soTimeout}; other timeouts are dropped;</li>
 * <li>a {@code toD}, {@code enrich} or dynamic {@code wireTap} sends {@code responseTimeout},
 * {@code connectTimeout}, {@code connectionRequestTimeout} and {@code soTimeout} to the
 * receiver as query parameters: the endpoint metadata does not list them, so Camel's
 * optimised dynamic send treats them as the receiver's.</li>
 * </ul>
 * Forms that fail on their own, such as {@code httpClient.socketTimeout} or a
 * {@code camel.component.http.socket-timeout} setting, are left to Camel. Only the route,
 * step and option names are logged, never a URI or value. The check never fails a route.
 */
public final class HttpTimeoutOptions {
    private static final Logger LOG = LoggerFactory.getLogger(HttpTimeoutOptions.class);
    private static final Map<String, String> DYNAMIC_REPLACEMENTS = Map.of(
            "responseTimeout", "httpClient.responseTimeout",
            "connectTimeout", "httpClient.connectTimeout",
            "connectionRequestTimeout", "httpClient.connectionRequestTimeout",
            "soTimeout", "httpConnection.soTimeout");
    private static final Map<String, String> COMPONENT_PROPERTIES = Map.of(
            "responseTimeout", "response-timeout",
            "connectTimeout", "connect-timeout",
            "connectionRequestTimeout", "connection-request-timeout",
            "soTimeout", "so-timeout");
    private static final Set<String> DROPPED_CONNECTION_OPTIONS = Set.of(
            "socketTimeout", "responseTimeout", "connectTimeout", "connectionRequestTimeout");
    private static final String SENT = "Camel 4's http component does not know it and sends it to the receiver "
            + "as a query parameter";
    private static final String SENT_DYNAMIC = "on a toD, enrich or dynamic wireTap, Camel 4's http component "
            + "sends it to the receiver as a query parameter";
    private static final String DROPPED = "Camel 4's http component applies only httpConnection.soTimeout and "
            + "drops other httpConnection timeouts";

    private final Set<String> warned = ConcurrentHashMap.newKeySet();

    /** Creates a check that warns once per route, step and option. */
    public HttpTimeoutOptions() {
    }

    /**
     * Checks one route node as Camel creates it.
     * @param context the Camel context, to resolve property placeholders
     * @param definition the node
     * @param target the node's processor, for its id when the node has none
     */
    public void check(CamelContext context, NamedNode definition, Processor target) {
        try {
            Send send = send(context, definition);
            if (send == null || !ForwardedCredentialHeaders.isHttp(send.uri())) {
                return;
            }
            String uri = send.uri().trim();
            String scheme = uri.substring(0, uri.indexOf(':')).toLowerCase(Locale.ROOT);
            String query = URISupport.extractQuery(uri);
            if (query == null) {
                return;
            }
            // Camel's own parser, so that an option inside a RAW(...) value is not taken for one.
            for (String option : URISupport.parseQuery(query, true, true).keySet()) {
                warnIfIgnored(option.trim(), send, scheme, definition, target);
            }
        } catch (RuntimeException | URISyntaxException e) {
            // A warning must never stop a route from being created.
        }
    }

    private void warnIfIgnored(String option, Send send, String scheme, NamedNode definition, Processor target) {
        String reason;
        String replacement;
        if (option.equals("socketTimeout")) {
            reason = SENT;
            replacement = send.optimised() ? DYNAMIC_REPLACEMENTS.get("responseTimeout") : "responseTimeout";
        } else if (option.startsWith("httpConnection.")
                && DROPPED_CONNECTION_OPTIONS.contains(option.substring("httpConnection.".length()))) {
            String name = option.substring("httpConnection.".length());
            String plain = name.equals("socketTimeout") ? "responseTimeout" : name;
            reason = DROPPED;
            replacement = send.optimised() ? DYNAMIC_REPLACEMENTS.get(plain) : plain;
        } else if (send.optimised() && DYNAMIC_REPLACEMENTS.containsKey(option)) {
            reason = SENT_DYNAMIC;
            replacement = DYNAMIC_REPLACEMENTS.get(option);
        } else {
            return;
        }
        String route = routeId(definition);
        String step = definition.getId() != null ? definition.getId()
                : target instanceof IdAware idAware ? idAware.getId() : null;
        if (!warned.add(route + "\n" + step + "\n" + option)) {
            return;
        }
        String base = replacement.substring(replacement.indexOf('.') + 1);
        LOG.warn("HTTP option {} on {} {} in route {} has no effect: {}. The call keeps the component's timeout, "
                        + "three minutes unless configured. Use {} on this endpoint, or camel.component.{}.{} for "
                        + "every call through the component",
                option, send.kind(), step, route, reason, replacement, scheme, COMPONENT_PROPERTIES.get(base));
    }

    /** The send's URI and whether Camel's optimised dynamic send handles it. */
    private static Send send(CamelContext context, NamedNode definition) {
        if (definition instanceof WireTapDefinition<?> wireTap) {
            String uri = resolve(context, uri(wireTap));
            boolean dynamic = !isFalse(context, wireTap.getDynamicUri()) && LanguageSupport.hasSimpleFunction(uri)
                    || isTrue(context, wireTap.getIgnoreInvalidEndpoint());
            return new Send("wireTap", uri, dynamic && !isFalse(context, wireTap.getAllowOptimisedComponents()));
        }
        if (definition instanceof ToDynamicDefinition toD) {
            return new Send("toD", resolve(context, uri(toD)), !isFalse(context, toD.getAllowOptimisedComponents()));
        }
        if (definition instanceof SendDefinition<?> to) {
            return new Send("to", resolve(context, to.getEndpointUri()), false);
        }
        if (definition instanceof EnrichDefinition enrich
                && (enrich.getExpression() instanceof ConstantExpression || enrich.getExpression() instanceof SimpleExpression)) {
            return new Send("enrich", resolve(context, enrich.getExpression().getExpression()),
                    !isFalse(context, enrich.getAllowOptimisedComponents()));
        }
        return null;
    }

    private static String uri(ToDynamicDefinition definition) {
        return definition.getEndpointProducerBuilder() != null
                ? definition.getEndpointProducerBuilder().getRawUri() : definition.getUri();
    }

    private static String resolve(CamelContext context, String uri) {
        if (uri == null) {
            return null;
        }
        try {
            return EndpointHelper.resolveEndpointUriPropertyPlaceholders(context, uri);
        } catch (RuntimeException e) {
            return uri;
        }
    }

    private static boolean isFalse(CamelContext context, String value) {
        return value != null && "false".equalsIgnoreCase(resolve(context, value).trim());
    }

    private static boolean isTrue(CamelContext context, String value) {
        return value != null && "true".equalsIgnoreCase(resolve(context, value).trim());
    }

    private static String routeId(NamedNode definition) {
        for (NamedNode node = definition; node != null; node = node.getParent()) {
            if (node instanceof RouteDefinition route) {
                return route.getId();
            }
        }
        return null;
    }

    private record Send(String kind, String uri, boolean optimised) {
    }
}
