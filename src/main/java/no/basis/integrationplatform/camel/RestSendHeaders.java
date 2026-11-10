package no.basis.integrationplatform.camel;

import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.Route;
import org.apache.camel.model.EndpointRequiredDefinition;
import org.apache.camel.model.ProcessorDefinitionHelper;
import org.apache.camel.spi.HeaderFilterStrategy;
import org.apache.camel.spi.HeaderFilterStrategyAware;
import org.apache.camel.spi.InterceptSendToEndpoint;
import org.apache.camel.spi.PropertyConfigurerGetter;
import org.apache.camel.model.ModelCamelContext;
import org.apache.camel.model.RouteDefinition;
import org.apache.camel.support.EndpointHelper;
import org.apache.camel.support.LifecycleStrategySupport;
import org.apache.camel.util.URISupport;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Keeps a consumed record's credential headers off the request of a {@code rest:} send. The
 * {@code rest:} producer creates its producer through the HTTP component's rest producer factory,
 * which registers the component's HTTP endpoint and then sets a filter of its own on it: the
 * producer parameter {@code headerFilterStrategy} if there is one, else the component's rest
 * filter, which keeps the headers bound into the path and query out of the request. So when a
 * {@code rest:} endpoint is registered, its producer parameter is seeded with the record filter
 * around the filter the component would have used: the application's own, or the component's
 * rest filter, built for the endpoint's path and query parameters. The exchange keeps its
 * headers, so the path and query bind as before.
 * <p>
 * A {@code rest:} endpoint that is a route's input, a REST DSL service, is left alone: its
 * consumer component reads the same parameters, and the filter must never become a consumer's.
 * So a route may not also send to that endpoint: {@link RouteCheck} refuses a route whose static
 * send targets a {@code rest:} endpoint another route consumes from, and a route that consumes
 * from a {@code rest:} endpoint that was set up for sends before its route was added.
 * <p>
 * Covered producer components: {@code http}, {@code vertx-http} and {@code netty-http}, also
 * subclassed. Their rest filters are loaded by name, since the adapter has no compile dependency
 * on the components. A {@code rest:} send through any other component fails when the filter is
 * first put on its endpoint, or at its first request, instead of sending unfiltered. Not
 * covered: a rest producer factory that ignores the {@code headerFilterStrategy} parameter.
 */
final class RestSendHeaders {
    static final String HEADER_FILTER_STRATEGY = "headerFilterStrategy";
    private static final String REST_ENDPOINT = "org.apache.camel.component.rest.RestEndpoint";
    // Each covered component and the rest filter its rest producer factory installs.
    private static final Map<String, String> REST_FILTERS = Map.of(
            "org.apache.camel.component.http.HttpComponent", "org.apache.camel.http.common.HttpRestHeaderFilterStrategy",
            "org.apache.camel.component.vertx.http.VertxHttpComponent",
            "org.apache.camel.component.vertx.http.VertxHttpRestHeaderFilterStrategy",
            "org.apache.camel.component.netty.http.NettyHttpComponent",
            "org.apache.camel.component.netty.http.NettyHttpRestHeaderFilterStrategy");

    private RestSendHeaders() {
    }

    static boolean isRestEndpoint(Endpoint endpoint) {
        return named(endpoint.getClass(), REST_ENDPOINT) != null;
    }

    /**
     * Puts the record filter into a {@code rest:} endpoint's producer parameters, around the
     * application's own filter if it set one.
     */
    static void seed(Endpoint rest) {
        if (isRouteInput(rest)) {
            return;
        }
        Map<String, Object> parameters = parameters(rest);
        Object own = parameters.get(HEADER_FILTER_STRATEGY);
        if (own instanceof ForwardedCredentialHeaders.RecordFilter) {
            return;
        }
        HeaderFilterStrategy filter = own == null ? new RestFilter(rest)
                : ForwardedCredentialHeaders.recordFilter(resolve(rest.getCamelContext(), own));
        try {
            parameters.put(HEADER_FILTER_STRATEGY, filter);
        } catch (RuntimeException e) {
            throw unfiltered(rest, e);
        }
        if (parameters.get(HEADER_FILTER_STRATEGY) != filter) {
            throw unfiltered(rest, null);
        }
    }

    /**
     * Binds a {@code rest:} send's filter to the component of the endpoint it was put on, when
     * the rest producer factory registers that endpoint with it.
     */
    static void bindIfRestFilter(Endpoint endpoint) {
        if (filterOf(endpoint) instanceof RestFilter filter) {
            filter.bind(endpoint);
        }
    }

    /** The endpoint's own {@code headerFilterStrategy} option, or null when it has none or cannot be read. */
    static Object filterOf(Endpoint endpoint) {
        try {
            if (endpoint.getComponent() != null
                    && endpoint.getComponent().getEndpointPropertyConfigurer() instanceof PropertyConfigurerGetter getter
                    && getter.getOptionType(HEADER_FILTER_STRATEGY, true) instanceof Class<?> type
                    && HeaderFilterStrategy.class.isAssignableFrom(type)) {
                return getter.getOptionValue(endpoint, HEADER_FILTER_STRATEGY, true);
            }
            return endpoint instanceof HeaderFilterStrategyAware aware ? aware.getHeaderFilterStrategy() : null;
        } catch (RuntimeException e) {
            return null;
        }
    }

    /**
     * Whether a route consumes from the endpoint. The REST DSL adds its services as routes
     * from {@code rest:} endpoints before Camel creates their endpoints.
     */
    static boolean isRouteInput(Endpoint rest) {
        if (!(rest.getCamelContext() instanceof ModelCamelContext model)) {
            return false;
        }
        String uri = normalized(rest.getCamelContext(), rest.getEndpointUri());
        for (RouteDefinition route : List.copyOf(model.getRouteDefinitions())) {
            String input = route.getInput() == null ? null : route.getInput().getEndpointUri();
            if (input != null && uri != null && uri.equals(normalized(rest.getCamelContext(), input))) {
                return true;
            }
        }
        return false;
    }

    private static String normalized(CamelContext context, String uri) {
        try {
            return URISupport.normalizeUri(context.resolvePropertyPlaceholders(uri));
        } catch (Exception e) {
            return uri;
        }
    }

    /**
     * Refuses, when a route is added, a send that would go through an unfiltered {@code rest:}
     * endpoint because a route consumes from it, and a consumer of a {@code rest:} endpoint
     * whose parameters carry the record filter.
     */
    static final class RouteCheck extends LifecycleStrategySupport {
        @Override
        public void onRoutesAdd(Collection<Route> added) {
            List<Route> routes = new ArrayList<>(added);
            if (!added.isEmpty()) {
                routes.addAll(added.iterator().next().getCamelContext().getRoutes());
            }
            Map<String, String> inputs = new HashMap<>();
            for (Route route : routes) {
                Endpoint endpoint = route.getEndpoint();
                while (endpoint instanceof InterceptSendToEndpoint intercepted && intercepted.getOriginalEndpoint() != endpoint) {
                    endpoint = intercepted.getOriginalEndpoint();
                }
                if (endpoint == null || !isRestEndpoint(endpoint)) {
                    continue;
                }
                if (parameters(endpoint).get(HEADER_FILTER_STRATEGY) instanceof ForwardedCredentialHeaders.RecordFilter) {
                    throw new IllegalStateException("Route " + route.getRouteId() + " consumes from the rest: endpoint "
                            + EndpointUriMasking.mask(endpoint.getEndpointUri()) + ", which was set up for rest: sends "
                            + "before the route was added, so its consumer would use the sends' header filter. Give the "
                            + "route's input and the sends different URIs, for example an explicit host= on the sends");
                }
                inputs.put(normalized(endpoint.getCamelContext(), endpoint.getEndpointUri()), route.getRouteId());
            }
            if (inputs.isEmpty() || !(routes.getFirst().getCamelContext() instanceof ModelCamelContext model)) {
                return;
            }
            for (RouteDefinition route : List.copyOf(model.getRouteDefinitions())) {
                for (EndpointRequiredDefinition send
                        : ProcessorDefinitionHelper.filterTypeInOutputs(route.getOutputs(), EndpointRequiredDefinition.class)) {
                    String uri = send.getEndpointUri();
                    String consumer = uri == null ? null : inputs.get(normalized(model, uri));
                    if (consumer != null) {
                        throw new IllegalStateException("Route " + route.getRouteId() + " sends to the rest: endpoint "
                                + EndpointUriMasking.mask(uri) + ", which route " + consumer + " consumes from. The "
                                + "adapter cannot keep a consumed Kafka record's credential headers off a send through "
                                + "a route's input endpoint; give the send a different URI, for example an explicit "
                                + "host=");
                    }
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> parameters(Endpoint rest) {
        Object parameters = call(rest, "getParameters");
        if (!(parameters instanceof Map<?, ?> map)) {
            throw unfiltered(rest, null);
        }
        return (Map<String, Object>) map;
    }

    /** As the rest producer factories resolve the parameter. */
    private static HeaderFilterStrategy resolve(CamelContext context, Object own) {
        if (own instanceof String text && EndpointHelper.isReferenceParameter(text)) {
            return EndpointHelper.resolveReferenceParameter(context, text, HeaderFilterStrategy.class);
        }
        if (own instanceof HeaderFilterStrategy strategy) {
            return strategy;
        }
        String text = context.getTypeConverter().tryConvertTo(String.class, own);
        return EndpointHelper.resolveReferenceParameter(context, text, HeaderFilterStrategy.class);
    }

    private static Object call(Object target, String getter) {
        try {
            Method method = target.getClass().getMethod(getter);
            return method.invoke(target);
        } catch (ReflectiveOperationException | RuntimeException e) {
            throw unfiltered((Endpoint) target, e);
        }
    }

    private static Class<?> named(Class<?> type, String name) {
        for (Class<?> c = type; c != null; c = c.getSuperclass()) {
            if (c.getName().equals(name)) {
                return c;
            }
        }
        return null;
    }

    private static IllegalStateException unfiltered(Endpoint rest, Throwable cause) {
        return new IllegalStateException("Cannot add a consumed Kafka record's credential headers and the Kafka "
                + "consumer's kafka.* headers to the header filter of the rest: send "
                + EndpointUriMasking.mask(rest.getEndpointUri()) + "; it could send them to its receiver", cause);
    }

    /**
     * The record filter around the rest filter of the component the send goes through, which
     * is known only once the rest producer factory has put this filter on its endpoint.
     */
    static final class RestFilter implements HeaderFilterStrategy, ForwardedCredentialHeaders.RecordFilter {
        private final Endpoint rest;
        private volatile HeaderFilterStrategy component;

        RestFilter(Endpoint rest) {
            this.rest = rest;
        }

        @Override
        public boolean applyFilterToCamelHeaders(String name, Object value, Exchange exchange) {
            HeaderFilterStrategy filter = componentFilter();
            return ForwardedCredentialHeaders.isRecordHeader(name, value, exchange)
                    || filter.applyFilterToCamelHeaders(name, value, exchange);
        }

        @Override
        public boolean applyFilterToExternalHeaders(String name, Object value, Exchange exchange) {
            return componentFilter().applyFilterToExternalHeaders(name, value, exchange);
        }

        boolean bound() {
            return component != null;
        }

        private HeaderFilterStrategy componentFilter() {
            HeaderFilterStrategy filter = component;
            if (filter != null) {
                return filter;
            }
            // The factory reused an endpoint registered before, so it was not bound then.
            for (Endpoint endpoint : List.copyOf(rest.getCamelContext().getEndpoints())) {
                while (endpoint instanceof InterceptSendToEndpoint intercepted && intercepted.getOriginalEndpoint() != endpoint) {
                    endpoint = intercepted.getOriginalEndpoint();
                }
                if (endpoint != null && filterOf(endpoint) == this) {
                    return bind(endpoint);
                }
            }
            throw unfiltered(rest, null);
        }

        synchronized HeaderFilterStrategy bind(Endpoint endpoint) {
            if (component != null) {
                return component;
            }
            String filterClass = null;
            Class<?> componentClass = endpoint.getComponent() == null ? null : endpoint.getComponent().getClass();
            for (Class<?> c = componentClass; c != null && filterClass == null; c = c.getSuperclass()) {
                filterClass = REST_FILTERS.get(c.getName());
            }
            if (filterClass == null) {
                throw new IllegalStateException("A rest: send through " + EndpointUriMasking.mask(endpoint.getEndpointUri())
                        + " is not covered: the adapter keeps a consumed Kafka record's credential headers off rest: "
                        + "sends through the http, vertx-http and netty-http components only; use one of them as the "
                        + "rest producer component");
            }
            try {
                Object uriTemplate = call(rest, "getUriTemplate");
                Object path = uriTemplate != null ? uriTemplate : call(rest, "getPath");
                Object query = call(rest, "getQueryParameters");
                Object filter = Class.forName(filterClass, true, componentClass.getClassLoader())
                        .getConstructor(String.class, String.class)
                        .newInstance(path, query);
                component = (HeaderFilterStrategy) filter;
                return component;
            } catch (ReflectiveOperationException | RuntimeException e) {
                throw unfiltered(rest, e);
            }
        }
    }
}
