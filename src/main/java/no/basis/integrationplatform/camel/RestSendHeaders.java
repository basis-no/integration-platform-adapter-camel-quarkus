package no.basis.integrationplatform.camel;

import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.spi.HeaderFilterStrategy;
import org.apache.camel.spi.HeaderFilterStrategyAware;
import org.apache.camel.spi.InterceptSendToEndpoint;
import org.apache.camel.spi.PropertyConfigurerGetter;
import org.apache.camel.support.EndpointHelper;

import java.lang.reflect.Method;
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
 * The consumer of a {@code rest:} endpoint that is a route's input, a REST DSL service, reads the
 * same parameters, and the filter must never become a consumer's. So the endpoint's parameters
 * are replaced by a view that shows the filter to every caller except Camel's
 * {@code RestEndpoint.createConsumer}, which sees them as they were. A send through an endpoint
 * that a route also consumes from is then filtered as any other.
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
        Map<String, Object> parameters = parameters(rest);
        if (parameters instanceof SendParameters) {
            return;
        }
        var view = new SendParameters(rest, parameters);
        try {
            rest.getClass().getMethod("setParameters", Map.class).invoke(rest, view);
        } catch (ReflectiveOperationException | RuntimeException e) {
            throw unfiltered(rest, e);
        }
        if (parameters(rest) != view || !(view.get(HEADER_FILTER_STRATEGY) instanceof ForwardedCredentialHeaders.RecordFilter)) {
            throw unfiltered(rest, null);
        }
    }

    /**
     * A {@code rest:} endpoint's parameters with the record filter as {@code headerFilterStrategy},
     * around the filter the parameters name, if any. Only Camel's consumer creation, which reads
     * them from {@code RestEndpoint.createConsumer}, sees them without it; every other reader,
     * the rest producer factories included, sees the filter. A snapshot is returned for
     * iteration, as the factories copy the parameters before they use them.
     */
    static final class SendParameters extends java.util.AbstractMap<String, Object> {
        private final Endpoint rest;
        private final Map<String, Object> parameters;
        private RestFilter restFilter;
        private Object own;
        private HeaderFilterStrategy composed;

        SendParameters(Endpoint rest, Map<String, Object> parameters) {
            this.rest = rest;
            this.parameters = parameters;
        }

        private static boolean consumerCreation() {
            return StackWalker.getInstance().walk(frames -> frames.anyMatch(
                    frame -> frame.getClassName().equals(REST_ENDPOINT) && frame.getMethodName().equals("createConsumer")));
        }

        private synchronized HeaderFilterStrategy sendFilter() {
            Object current = parameters.get(HEADER_FILTER_STRATEGY);
            if (current == null) {
                if (restFilter == null) {
                    restFilter = new RestFilter(rest);
                }
                return restFilter;
            }
            if (composed == null || current != own) {
                composed = ForwardedCredentialHeaders.recordFilter(resolve(rest.getCamelContext(), current));
                own = current;
            }
            return composed;
        }

        private Map<String, Object> seen() {
            if (consumerCreation()) {
                return parameters;
            }
            Map<String, Object> copy = new java.util.LinkedHashMap<>(parameters);
            copy.put(HEADER_FILTER_STRATEGY, sendFilter());
            return copy;
        }

        @Override
        public java.util.Set<Entry<String, Object>> entrySet() {
            return java.util.Collections.unmodifiableMap(seen()).entrySet();
        }

        @Override
        public Object get(Object key) {
            return HEADER_FILTER_STRATEGY.equals(key) ? seen().get(key) : parameters.get(key);
        }

        @Override
        public boolean containsKey(Object key) {
            return HEADER_FILTER_STRATEGY.equals(key) ? seen().containsKey(key) : parameters.containsKey(key);
        }

        @Override
        public Object put(String key, Object value) {
            return parameters.put(key, value);
        }

        @Override
        public Object remove(Object key) {
            // The filter stays in place for sends; a factory removes it only from its own copy.
            return HEADER_FILTER_STRATEGY.equals(key) && !consumerCreation() ? sendFilter() : parameters.remove(key);
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
