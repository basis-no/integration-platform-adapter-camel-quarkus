package no.basis.integrationplatform.camel.step;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.camel.model.LoopDefinition;
import org.apache.camel.model.MulticastDefinition;
import org.apache.camel.model.ProcessorDefinition;
import org.apache.camel.model.RecipientListDefinition;
import org.apache.camel.model.RouteDefinition;
import org.apache.camel.model.SplitDefinition;
import org.apache.camel.util.URISupport;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import no.basis.integrationplatform.camel.ForwardedCredentialHeaders;
import no.basis.integrationplatform.camel.HttpTimeoutOptions;
import no.basis.integrationplatform.camel.PlatformExchangeProperties;
import no.basis.integrationplatform.camel.PublishedCredentialHeaders;
import org.apache.camel.AsyncCallback;
import org.apache.camel.AsyncProcessor;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePropertyKey;
import org.apache.camel.NamedNode;
import org.apache.camel.Processor;
import org.apache.camel.processor.SendDynamicProcessor;
import org.apache.camel.processor.SendProcessor;
import org.apache.camel.spi.HeaderFilterStrategy;
import org.apache.camel.spi.InterceptStrategy;
import org.apache.camel.support.AsyncProcessorConverterHelper;
import org.apache.camel.support.processor.DelegateAsyncProcessor;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/** Wraps endpoint-send processors with automatic classification and step tracking. */
@ApplicationScoped
public class StepTrackingInterceptStrategy implements InterceptStrategy {
    /** Emergency escape hatch: start with generated step ids, logging a warning for each. */
    public static final String EMERGENCY_ALLOW_GENERATED_STEP_IDS = "basis.message-store.step-ids.emergency-allow-generated";
    private static final Logger LOG = LoggerFactory.getLogger(StepTrackingInterceptStrategy.class);

    private final StepTracker stepTracker;
    private final boolean allowGeneratedStepIds;
    private final AtomicLong scopes = new AtomicLong();
    private final HttpTimeoutOptions httpTimeoutOptions = new HttpTimeoutOptions();
    private final PublishedCredentialHeaders publishedCredentials = new PublishedCredentialHeaders();

    /**
     * Creates the intercept strategy for programmatic use: generated step ids log a warning.
     *
     * @param stepTracker tracker that owns classification and durable outcomes
     */
    public StepTrackingInterceptStrategy(StepTracker stepTracker) {
        this(stepTracker, true);
    }

    /**
     * Creates the intercept strategy.
     *
     * @param stepTracker tracker that owns classification and durable outcomes
     * @param allowGeneratedStepIds {@code true} only as an emergency: a send without an explicit
     *        id then logs a warning instead of failing route creation
     */
    @Inject
    public StepTrackingInterceptStrategy(StepTracker stepTracker,
            @ConfigProperty(name = EMERGENCY_ALLOW_GENERATED_STEP_IDS, defaultValue = "false") boolean allowGeneratedStepIds) {
        this.stepTracker = stepTracker;
        this.allowGeneratedStepIds = allowGeneratedStepIds;
    }

    @Override
    public Processor wrapProcessorInInterceptors(
            CamelContext context,
            NamedNode definition,
            Processor target,
            Processor nextTarget) {

        httpTimeoutOptions.check(context, definition, target);
        EndpointSend endpointSend = endpointSend(definition, target);
        if (endpointSend == null) {
            String kind = scopeKind(definition);
            return kind == null ? target
                    : new PartsScope(AsyncProcessorConverterHelper.convert(target), StepVisits.marker(kind, scopes.incrementAndGet()));
        }
        checkStepId(definition, endpointSend);

        AsyncProcessor delegate = AsyncProcessorConverterHelper.convert(target);
        return new StepTrackingProcessor(delegate, endpointSend, stepTracker, dynamicScheme(target, endpointSend),
                PublishedCredentialHeaders.kafkaFilter(target, endpointSend.endpointUri()), routeId(definition),
                publishedCredentials);
    }

    /**
     * Whether a {@code toD} chooses its component only when it sends, such as
     * {@code ${header.receiverUrl}} or {@code language:header:receiverUrl}. Its expression is
     * not evaluated here: an expression may change on each evaluation, and Camel evaluates
     * it once for the send.
     */
    private static boolean dynamicScheme(Processor target, EndpointSend endpointSend) {
        if (!(target instanceof SendDynamicProcessor) || endpointSend.endpointUri() == null) {
            return false;
        }
        String uri = endpointSend.endpointUri().trim();
        int colon = uri.indexOf(':');
        String scheme = colon < 0 ? uri : uri.substring(0, colon);
        return scheme.contains("$") || scheme.contains("{") || scheme.equalsIgnoreCase("language");
    }

    /** The kind of a node whose parts Camel runs as copies of the exchange, or as iterations of it. */
    private static String scopeKind(NamedNode definition) {
        if (definition instanceof SplitDefinition) return StepVisits.SPLIT;
        if (definition instanceof LoopDefinition) return StepVisits.LOOP;
        if (definition instanceof MulticastDefinition) return StepVisits.MULTICAST;
        if (definition instanceof RecipientListDefinition<?>) return StepVisits.RECIPIENT_LIST;
        return null;
    }

    /**
     * Shares one record of the attempt's step visits with the parts of a split, loop,
     * multicast or recipient list, and marks the exchange so that each part gets its own
     * position. Camel may copy a part's properties back afterwards, so the
     * caller's own record and path are restored. When the scope changed Camel's loop index
     * inside an enclosing loop, that loop's iteration is no longer known. Only these adapter
     * properties change: the parts, the outcome and the error handling are Camel's.
     */
    private static final class PartsScope extends DelegateAsyncProcessor {
        private final String marker;

        private PartsScope(AsyncProcessor processor, String marker) {
            super(processor);
            this.marker = marker;
        }

        @Override
        public boolean process(Exchange exchange, AsyncCallback callback) {
            StepVisits visits = StepVisits.of(exchange);
            String parent = StepVisits.path(exchange);
            String path = exchange.getProperty(StepVisits.PATH, String.class);
            String enclosing = exchange.getProperty(StepVisits.SCOPE, String.class);
            String unknown = exchange.getProperty(StepVisits.LOOP_UNKNOWN, String.class);
            Object loopIndex = exchange.getProperty(ExchangePropertyKey.LOOP_INDEX);
            exchange.setProperty(StepVisits.PATH, parent);
            exchange.setProperty(StepVisits.SCOPE, marker);
            return super.process(exchange, doneSync -> {
                exchange.setProperty(StepVisits.PROPERTY, visits);
                restore(exchange, StepVisits.PATH, path);
                restore(exchange, StepVisits.SCOPE, enclosing);
                restore(exchange, StepVisits.LOOP_UNKNOWN, unknown);
                if (StepVisits.isLoop(enclosing) && !Objects.equals(loopIndex, exchange.getProperty(ExchangePropertyKey.LOOP_INDEX))) {
                    exchange.setProperty(StepVisits.LOOP_UNKNOWN, enclosing);
                }
                callback.done(doneSync);
            });
        }

        private static void restore(Exchange exchange, String name, String value) {
            if (value == null) {
                exchange.removeProperty(name);
            } else {
                exchange.setProperty(name, value);
            }
        }
    }

    /**
     * The step id is the node id. A generated one such as {@code to1} changes when the route
     * is edited and repeats across integrations, and node-id classification cannot name it.
     * Route creation fails unless the emergency flag allows it with a warning.
     */
    private void checkStepId(NamedNode definition, EndpointSend endpointSend) {
        if (!(definition instanceof ProcessorDefinition<?> processor) || processor.hasCustomIdAssigned()
                || DefaultStepClassifier.isInternalScheme(endpointSend.endpointUri())) {
            return;
        }
        String routeId = routeId(definition);
        String endpoint = URISupport.sanitizeUri(endpointSend.endpointUri());
        if (!allowGeneratedStepIds) {
            throw new IllegalStateException("Endpoint send to " + endpoint + " in route " + routeId
                    + " has no explicit id; its step id would be the generated " + endpointSend.nodeId()
                    + ". Give the send an explicit id. Only in an emergency, set "
                    + EMERGENCY_ALLOW_GENERATED_STEP_IDS + "=true to start with a warning instead");
        }
        LOG.warn("Endpoint send to {} in route {} has no explicit id; its step id {} is generated, changes when the "
                        + "route is edited and can match another integration's step for the same message id. "
                        + "Give the send an explicit id ({}=true is set)",
                endpoint, routeId, endpointSend.nodeId(), EMERGENCY_ALLOW_GENERATED_STEP_IDS);
    }

    private static String routeId(NamedNode definition) {
        for (NamedNode node = definition; node != null; node = node.getParent()) {
            if (node instanceof RouteDefinition route) {
                return route.getId();
            }
        }
        return null;
    }

    private EndpointSend endpointSend(NamedNode definition, Processor target) {
        if (target instanceof SendProcessor sendProcessor) {
            String endpointUri = sendProcessor.getEndpoint().getEndpointUri();
            return new EndpointSend(endpointUri, nodeId(definition, sendProcessor.getId()));
        }

        if (target instanceof SendDynamicProcessor sendDynamicProcessor) {
            return new EndpointSend(sendDynamicProcessor.getUri(), nodeId(definition, sendDynamicProcessor.getId()));
        }

        return null;
    }

    private String nodeId(NamedNode definition, String processorId) {
        if (definition != null && definition.getId() != null && !definition.getId().isBlank()) {
            return definition.getId();
        }
        return processorId;
    }

    record EndpointSend(String endpointUri, String nodeId) {
    }

    static class StepTrackingProcessor implements AsyncProcessor {
        private final AsyncProcessor delegate;
        private final EndpointSend endpointSend;
        private final StepTracker stepTracker;
        private final boolean httpSend;
        private final boolean dynamicScheme;
        private final HeaderFilterStrategy kafkaFilter;
        private final String routeId;
        private final PublishedCredentialHeaders publishedCredentials;

        StepTrackingProcessor(AsyncProcessor delegate, EndpointSend endpointSend, StepTracker stepTracker) {
            this(delegate, endpointSend, stepTracker, false);
        }

        StepTrackingProcessor(AsyncProcessor delegate, EndpointSend endpointSend, StepTracker stepTracker,
                boolean dynamicScheme) {
            this(delegate, endpointSend, stepTracker, dynamicScheme, null, null, null);
        }

        StepTrackingProcessor(AsyncProcessor delegate, EndpointSend endpointSend, StepTracker stepTracker,
                boolean dynamicScheme, HeaderFilterStrategy kafkaFilter, String routeId,
                PublishedCredentialHeaders publishedCredentials) {
            this.delegate = delegate;
            this.endpointSend = endpointSend;
            this.stepTracker = stepTracker;
            this.httpSend = ForwardedCredentialHeaders.isHttp(endpointSend.endpointUri());
            this.dynamicScheme = dynamicScheme;
            this.kafkaFilter = kafkaFilter;
            this.routeId = routeId;
            this.publishedCredentials = publishedCredentials;
        }

        @Override
        public void process(Exchange exchange) throws Exception {
            stepTracker.beforeSend(exchangeWithInterceptProperties(exchange));
            if (alreadyDelivered(exchange)) {
                stepTracker.restoreSkippedOutput(exchange);
                stepTracker.recordSkipped(exchange);
                return;
            }
            warnAboutPublishedCredentials(exchange);
            var withheld = withholdCredentials(exchange);
            try {
                try { delegate.process(exchange); }
                finally { ForwardedCredentialHeaders.restore(exchange, withheld); }
                if (exchange.getException() == null) {
                    stepTracker.recordDelivered(exchange);
                } else {
                    stepTracker.recordFailed(exchange);
                }
            } catch (Exception e) {
                exchange.setProperty(Exchange.EXCEPTION_CAUGHT, e);
                stepTracker.recordFailed(exchange);
                throw e;
            }
        }

        @Override
        public boolean process(Exchange exchange, AsyncCallback callback) {
            try {
                stepTracker.beforeSend(exchangeWithInterceptProperties(exchange));
                if (alreadyDelivered(exchange)) {
                    stepTracker.restoreSkippedOutput(exchange);
                    stepTracker.recordSkipped(exchange);
                    callback.done(true);
                    return true;
                }
            } catch (Exception failure) {
                exchange.setException(failure);
                callback.done(true);
                return true;
            }

            warnAboutPublishedCredentials(exchange);
            var withheld = withholdCredentials(exchange);
            return delegate.process(exchange, doneSync -> {
                try {
                    ForwardedCredentialHeaders.restore(exchange, withheld);
                    if (exchange.getException() == null) {
                        stepTracker.recordDelivered(exchange);
                    } else {
                        stepTracker.recordFailed(exchange);
                    }
                } catch (Exception failure) {
                    exchange.setException(failure);
                } finally {
                    callback.done(doneSync);
                }
            });
        }

        private Exchange exchangeWithInterceptProperties(Exchange exchange) {
            exchange.setProperty(Exchange.INTERCEPTED_ENDPOINT, endpointSend.endpointUri());
            exchange.setProperty(Exchange.INTERCEPTED_NODE_ID, endpointSend.nodeId());
            return exchange;
        }

        @Override
        public java.util.concurrent.CompletableFuture<Exchange> processAsync(Exchange exchange) {
            try {
                stepTracker.beforeSend(exchangeWithInterceptProperties(exchange));
                if (alreadyDelivered(exchange)) {
                    stepTracker.restoreSkippedOutput(exchange);
                    stepTracker.recordSkipped(exchange);
                    return java.util.concurrent.CompletableFuture.completedFuture(exchange);
                }
            } catch (Exception failure) {
                exchange.setException(failure);
                return java.util.concurrent.CompletableFuture.failedFuture(failure);
            }
            warnAboutPublishedCredentials(exchange);
            var withheld = withholdCredentials(exchange);
            return delegate.processAsync(exchange).whenComplete((result, failure) -> {
                Exchange completedExchange = result != null ? result : exchange;
                ForwardedCredentialHeaders.restore(completedExchange, withheld);
                if (failure == null && completedExchange.getException() == null) {
                    stepTracker.recordDelivered(completedExchange);
                } else {
                    if (failure != null) {
                        completedExchange.setProperty(Exchange.EXCEPTION_CAUGHT, failure);
                    }
                    stepTracker.recordFailed(completedExchange);
                }
            });
        }

        private void warnAboutPublishedCredentials(Exchange exchange) {
            if (kafkaFilter != null && publishedCredentials != null) {
                publishedCredentials.check(exchange, kafkaFilter, routeId, endpointSend.nodeId());
            }
        }

        private java.util.Map<String, Object> withholdCredentials(Exchange exchange) {
            // A send that picks its component at send time may be http, so it is treated as one.
            return httpSend || dynamicScheme
                    ? ForwardedCredentialHeaders.withhold(exchange, endpointSend.nodeId()) : java.util.Map.of();
        }

        private boolean alreadyDelivered(Exchange exchange) {
            return Boolean.TRUE.equals(exchange.getProperty(PlatformExchangeProperties.STEP_ALREADY_DELIVERED, Boolean.class));
        }
    }
}
