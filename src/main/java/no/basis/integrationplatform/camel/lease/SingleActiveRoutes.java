package no.basis.integrationplatform.camel.lease;

import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.model.FromDefinition;
import org.apache.camel.model.RouteDefinition;
import org.apache.camel.support.ScheduledPollEndpoint;

import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Decides which routes the adapter runs on one replica at a time.
 *
 * <p>A route is single-active when its consumer polls on a schedule: its
 * endpoint is a {@link ScheduledPollEndpoint} (file and FTP directories,
 * database queries, mailboxes, object stores, {@code scheduler:}) or its scheme
 * is {@code timer}, {@code quartz} or {@code cron}. A {@code master:} route
 * already coordinates itself and is left alone, as is every other consumer:
 * Kafka consumer groups, HTTP, broker queues, {@code direct:} and
 * {@code seda:}.</p>
 *
 * <p>Every such route is coordinated unless its id is listed in
 * {@value #EXCLUDED_ROUTES}; an excluded route polls on every replica.</p>
 */
public final class SingleActiveRoutes {
    /** Comma-separated ids of polling routes that run on every replica instead of on one. */
    public static final String EXCLUDED_ROUTES = "basis.message-store.single-active.excluded-routes";

    private static final Set<String> SCHEDULED_SCHEMES = Set.of("timer", "quartz", "cron", "scheduler");
    private static final String MASTER_SCHEME = "master";

    private SingleActiveRoutes() {
    }

    /**
     * Whether the adapter coordinates the route.
     *
     * @param camelContext context that resolves placeholders and the endpoint
     * @param route route definition
     * @return whether the route's consumer polls on a schedule and is not a {@code master:} route
     */
    public static boolean coordinates(CamelContext camelContext, RouteDefinition route) {
        FromDefinition input = route.getInput();
        if (input == null) {
            return false;
        }
        Endpoint endpoint = input.getEndpoint();
        String uri = endpoint != null ? endpoint.getEndpointUri() : input.getEndpointUri();
        if (uri == null) {
            return false;
        }
        uri = camelContext.resolvePropertyPlaceholders(uri);
        String scheme = scheme(uri);
        if (MASTER_SCHEME.equals(scheme)) {
            return false;
        }
        if (SCHEDULED_SCHEMES.contains(scheme)) {
            return true;
        }
        if (endpoint == null) {
            // The route resolved this endpoint already, so this is a registry lookup of the same instance.
            endpoint = camelContext.getEndpoint(uri);
        }
        return endpoint instanceof ScheduledPollEndpoint;
    }

    /**
     * Whether the adapter coordinates the route, given the route ids excluded from coordination.
     *
     * @param camelContext context that resolves placeholders and the endpoint
     * @param routeId stable Camel route id
     * @param route route definition
     * @param excluded route ids listed in {@value #EXCLUDED_ROUTES}
     * @return whether the route polls on a schedule, is not a {@code master:} route and is not excluded
     */
    public static boolean coordinates(CamelContext camelContext, String routeId, RouteDefinition route,
                                      Set<String> excluded) {
        return !excluded.contains(routeId) && coordinates(camelContext, route);
    }

    /**
     * Parses {@value #EXCLUDED_ROUTES}.
     *
     * @param configured the configured value, if any
     * @return the listed route ids
     */
    public static Set<String> excludedRoutes(Optional<String> configured) {
        return configured.map(value -> Arrays.stream(value.split(","))
                        .map(String::trim)
                        .filter(id -> !id.isEmpty())
                        .collect(Collectors.toUnmodifiableSet()))
                .orElse(Set.of());
    }

    private static String scheme(String uri) {
        int separator = uri.indexOf(':');
        return (separator < 0 ? uri : uri.substring(0, separator)).trim().toLowerCase(Locale.ROOT);
    }
}
