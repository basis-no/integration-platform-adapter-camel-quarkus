package no.basis.integrationplatform.camel.step;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import no.basis.integrationplatform.camel.traffic.TrafficSignals;
import no.basis.integrationplatform.camel.ForwardedCredentialHeaders;
import no.basis.integrationplatform.camel.PlatformExchangeProperties;
import org.apache.camel.AsyncCallback;
import org.apache.camel.AsyncProcessor;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.NamedNode;
import org.apache.camel.Processor;
import org.apache.camel.model.ProcessorDefinition;
import org.apache.camel.model.RouteDefinition;
import org.apache.camel.processor.SendDynamicProcessor;
import org.apache.camel.processor.SendProcessor;
import org.apache.camel.spi.InterceptStrategy;
import org.apache.camel.support.AsyncProcessorConverterHelper;
import org.apache.camel.util.URISupport;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class StepTrackingInterceptStrategy implements InterceptStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(StepTrackingInterceptStrategy.class);
    /** Emergency escape hatch: start with generated step ids, logging a warning for each. */
    public static final String EMERGENCY_ALLOW_GENERATED_STEP_IDS = "basis.message-store.step-ids.emergency-allow-generated";

    private final StepTracker stepTracker;
    private final TrafficSignals trafficSignals;
    private final boolean allowGeneratedStepIds;

    public StepTrackingInterceptStrategy(StepTracker stepTracker) {
        this(stepTracker, null);
    }

    /**
     * Creates tracking with optional existing-registry activity export. Programmatic
     * construction warns about generated step ids; the CDI runtime refuses them.
     * @param stepTracker durable step tracker
     * @param trafficSignals optional activity signal exporter
     */
    public StepTrackingInterceptStrategy(StepTracker stepTracker, TrafficSignals trafficSignals) {
        this(stepTracker, trafficSignals, true);
    }

    /**
     * Creates tracking.
     * @param stepTracker durable step tracker
     * @param trafficSignals optional activity signal exporter
     * @param allowGeneratedStepIds {@code true} only as an emergency: a tracked send without
     *        an explicit id then logs a warning instead of failing route creation
     */
    @Inject
    public StepTrackingInterceptStrategy(StepTracker stepTracker, TrafficSignals trafficSignals,
            @ConfigProperty(name = EMERGENCY_ALLOW_GENERATED_STEP_IDS, defaultValue = "false") boolean allowGeneratedStepIds) {
        this.stepTracker = stepTracker;
        this.trafficSignals = trafficSignals;
        this.allowGeneratedStepIds = allowGeneratedStepIds;
    }

    @Override
    public Processor wrapProcessorInInterceptors(
            CamelContext context,
            NamedNode definition,
            Processor target,
            Processor nextTarget) {

        // Parking is infrastructure work, not a replay-skippable business step.
        for (NamedNode node = definition; node != null; node = node.getParent()) {
            if (node instanceof RouteDefinition && "bip-confirmed-dead-letter".equals(node.getId())) {
                return target;
            }
        }
        EndpointSend endpointSend = endpointSend(definition, target);
        if (endpointSend == null) {
            return target;
        }

        checkStepId(definition, endpointSend);
        if (trafficSignals != null) {
            if (target instanceof SendProcessor send) trafficSignals.observeProducerEndpoint(send.getEndpoint());
            else if (target instanceof SendDynamicProcessor send) trafficSignals.observeDynamicEndpoint(send.getUri());
        }
        AsyncProcessor delegate = AsyncProcessorConverterHelper.convert(target);
        return new StepTrackingProcessor(delegate, endpointSend, stepTracker, trafficSignals);
    }

    /**
     * The step id is the node id. A generated one such as {@code to1} changes when the
     * route is edited and is the same in other integrations, so another integration's
     * delivered step with the same message id makes this send look already delivered.
     * Route creation fails unless the emergency flag allows it with a warning.
     */
    private void checkStepId(NamedNode definition, EndpointSend endpointSend) {
        if (!(definition instanceof ProcessorDefinition<?> processor) || processor.hasCustomIdAssigned()
                || DefaultStepTracker.isInternal(endpointSend.endpointUri())) {
            return;
        }
        String routeId = null;
        for (NamedNode node = definition; node != null && routeId == null; node = node.getParent()) {
            if (node instanceof RouteDefinition route) {
                routeId = route.getId();
            }
        }
        String endpoint = URISupport.sanitizeUri(endpointSend.endpointUri());
        if (!allowGeneratedStepIds) {
            throw new IllegalStateException("Tracked send to " + endpoint + " in route " + routeId
                    + " has no explicit id; its step id would be the generated " + endpointSend.nodeId()
                    + ". Give the send an explicit id. Only in an emergency, set "
                    + EMERGENCY_ALLOW_GENERATED_STEP_IDS + "=true to start with a warning instead");
        }
        LOG.warn("Tracked send to {} in route {} has no explicit id; its step id {} is generated, changes when the "
                        + "route is edited and can match another integration's step for the same message id. "
                        + "Give the send an explicit id ({}=true is set)",
                endpoint, routeId, endpointSend.nodeId(), EMERGENCY_ALLOW_GENERATED_STEP_IDS);
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

    private record EndpointSend(String endpointUri, String nodeId) {
    }

    private static class StepTrackingProcessor implements AsyncProcessor {
        private final AsyncProcessor delegate;
        private final EndpointSend endpointSend;
        private final StepTracker stepTracker;
        private final TrafficSignals trafficSignals;
        private final boolean httpSend;

        private StepTrackingProcessor(AsyncProcessor delegate, EndpointSend endpointSend, StepTracker stepTracker) {
            this(delegate, endpointSend, stepTracker, null);
        }

        private StepTrackingProcessor(AsyncProcessor delegate, EndpointSend endpointSend,
                StepTracker stepTracker, TrafficSignals trafficSignals) {
            this.delegate = delegate;
            this.endpointSend = endpointSend;
            this.stepTracker = stepTracker;
            this.trafficSignals = trafficSignals;
            this.httpSend = ForwardedCredentialHeaders.isHttp(endpointSend.endpointUri());
        }

        @Override
        public void process(Exchange exchange) throws Exception {
            stepTracker.beforeSend(exchangeWithInterceptProperties(exchange));
            if (alreadyDelivered(exchange)) {
                stepTracker.recordSkipped(exchange);
                return;
            }
            var trafficSnapshot = beforePublish(exchange);
            var withheld = withholdCredentials(exchange);
            try {
                boolean returned = false;
                try { delegate.process(exchange); returned = true; }
                finally {
                    ForwardedCredentialHeaders.restore(exchange, withheld);
                    afterPublish(trafficSnapshot, exchange, returned && exchange.getException() == null);
                }
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
            stepTracker.beforeSend(exchangeWithInterceptProperties(exchange));
            if (alreadyDelivered(exchange)) {
                stepTracker.recordSkipped(exchange);
                callback.done(true);
                return true;
            }

            var trafficSnapshot = beforePublish(exchange);
            var withheld = withholdCredentials(exchange);
            return delegate.process(exchange, doneSync -> {
                ForwardedCredentialHeaders.restore(exchange, withheld);
                afterPublish(trafficSnapshot, exchange, exchange.getException() == null);
                if (exchange.getException() == null) {
                    stepTracker.recordDelivered(exchange);
                } else {
                    stepTracker.recordFailed(exchange);
                }
                callback.done(doneSync);
            });
        }

        private Exchange exchangeWithInterceptProperties(Exchange exchange) {
            exchange.setProperty(Exchange.INTERCEPTED_ENDPOINT, endpointSend.endpointUri());
            exchange.setProperty(Exchange.INTERCEPTED_NODE_ID, endpointSend.nodeId());
            return exchange;
        }

        @Override
        public java.util.concurrent.CompletableFuture<Exchange> processAsync(Exchange exchange) {
            stepTracker.beforeSend(exchangeWithInterceptProperties(exchange));
            if (alreadyDelivered(exchange)) {
                stepTracker.recordSkipped(exchange);
                return java.util.concurrent.CompletableFuture.completedFuture(exchange);
            }
            var trafficSnapshot = beforePublish(exchange);
            var withheld = withholdCredentials(exchange);
            return delegate.processAsync(exchange).whenComplete((result, failure) -> {
                Exchange completedExchange = result != null ? result : exchange;
                ForwardedCredentialHeaders.restore(completedExchange, withheld);
                afterPublish(trafficSnapshot, completedExchange, failure == null && completedExchange.getException() == null);
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

        private java.util.Map<String, Object> withholdCredentials(Exchange exchange) {
            return httpSend ? ForwardedCredentialHeaders.withhold(exchange, endpointSend.nodeId()) : java.util.Map.of();
        }

        private TrafficSignals.PublishSnapshot beforePublish(Exchange exchange) {
            return trafficSignals == null ? null : trafficSignals.beforePublish(endpointSend.endpointUri(), exchange);
        }

        private void afterPublish(TrafficSignals.PublishSnapshot snapshot, Exchange exchange, boolean succeeded) {
            if (trafficSignals != null) trafficSignals.afterPublish(snapshot, exchange, succeeded);
        }

        private boolean alreadyDelivered(Exchange exchange) {
            return Boolean.TRUE.equals(exchange.getProperty(PlatformExchangeProperties.STEP_ALREADY_DELIVERED, Boolean.class));
        }
    }
}
