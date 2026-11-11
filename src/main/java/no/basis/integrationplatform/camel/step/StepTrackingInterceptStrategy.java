package no.basis.integrationplatform.camel.step;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.camel.model.LoopDefinition;
import org.apache.camel.model.MulticastDefinition;
import org.apache.camel.model.ProcessorDefinition;
import org.apache.camel.model.RecipientListDefinition;
import org.apache.camel.model.RouteDefinition;
import org.apache.camel.model.SplitDefinition;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import no.basis.integrationplatform.camel.EndpointUriMasking;
import no.basis.integrationplatform.camel.ForwardedCredentialHeaders;
import no.basis.integrationplatform.camel.HttpTimeoutOptions;
import no.basis.integrationplatform.camel.PlatformDeadLetterRoute;
import no.basis.integrationplatform.camel.PlatformExchangeProperties;
import no.basis.integrationplatform.camel.PublishedCredentialHeaders;
import no.basis.integrationplatform.camel.traffic.TrafficSignals;
import org.apache.camel.AsyncCallback;
import org.apache.camel.AsyncProcessor;
import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePropertyKey;
import org.apache.camel.NamedNode;
import org.apache.camel.Processor;
import org.apache.camel.processor.SendDynamicProcessor;
import org.apache.camel.processor.SendProcessor;
import org.apache.camel.spi.InterceptStrategy;
import org.apache.camel.support.AsyncCallbackToCompletableFutureAdapter;
import org.apache.camel.support.AsyncProcessorConverterHelper;
import org.apache.camel.support.PluginHelper;
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
    private final TrafficSignals trafficSignals;
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
        this(stepTracker, null, true);
    }

    /**
     * Creates the intercept strategy for programmatic use with activity counting: generated
     * step ids log a warning.
     *
     * @param stepTracker tracker that owns classification and durable outcomes
     * @param trafficSignals activity counters for Kafka publications, or {@code null}
     */
    public StepTrackingInterceptStrategy(StepTracker stepTracker, TrafficSignals trafficSignals) {
        this(stepTracker, trafficSignals, true);
    }

    /**
     * Creates the intercept strategy without activity counting.
     *
     * @param stepTracker tracker that owns classification and durable outcomes
     * @param allowGeneratedStepIds {@code true} only as an emergency: a send without an explicit
     *        id then logs a warning instead of failing route creation
     */
    public StepTrackingInterceptStrategy(StepTracker stepTracker, boolean allowGeneratedStepIds) {
        this(stepTracker, null, allowGeneratedStepIds);
    }

    /**
     * Creates the intercept strategy.
     *
     * @param stepTracker tracker that owns classification and durable outcomes
     * @param trafficSignals activity counters for Kafka publications, or {@code null}
     * @param allowGeneratedStepIds {@code true} only as an emergency: a send without an explicit
     *        id then logs a warning instead of failing route creation
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

        httpTimeoutOptions.check(context, definition, target);
        ForwardedCredentialHeaders.install(context);
        // Camel warns at route start for each processor handed back without an AsyncProcessor,
        // and bridges it itself; the converter returns an asynchronous processor unchanged.
        AsyncProcessor processor = AsyncProcessorConverterHelper.convert(target);
        if (inDeadLetterRoute(definition)) {
            // Parking is the adapter's own work, not a step that a replay may skip.
            return processor;
        }
        EndpointSend endpointSend = endpointSend(definition, target);
        if (endpointSend == null) {
            String kind = scopeKind(definition);
            return kind == null ? processor : new PartsScope(processor, StepVisits.marker(kind, scopes.incrementAndGet()));
        }
        checkStepId(definition, endpointSend);
        if (trafficSignals != null) {
            if (target instanceof SendProcessor send) trafficSignals.observeProducerEndpoint(send.getEndpoint());
            else if (target instanceof SendDynamicProcessor send) trafficSignals.observeDynamicEndpoint(send.getUri());
        }

        boolean dynamicScheme = dynamicScheme(target, endpointSend);
        Endpoint endpoint = target instanceof SendProcessor send ? send.getEndpoint() : null;
        boolean mayPublishToKafka = PublishedCredentialHeaders.mayPublishToKafka(
                endpoint, endpointSend.endpointUri(), dynamicScheme);
        return new StepTrackingProcessor(processor, endpointSend, stepTracker, trafficSignals, dynamicScheme,
                mayPublishToKafka ? endpoint : null, mayPublishToKafka, routeId(definition), publishedCredentials);
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
                try {
                    exchange.setProperty(StepVisits.PROPERTY, visits);
                    restore(exchange, StepVisits.PATH, path);
                    restore(exchange, StepVisits.SCOPE, enclosing);
                    restore(exchange, StepVisits.LOOP_UNKNOWN, unknown);
                    if (StepVisits.isLoop(enclosing) && !Objects.equals(loopIndex, exchange.getProperty(ExchangePropertyKey.LOOP_INDEX))) {
                        exchange.setProperty(StepVisits.LOOP_UNKNOWN, enclosing);
                    }
                } catch (Throwable failure) {
                    markFailed(exchange, failure);
                } finally {
                    callback.done(doneSync);
                }
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
     * is edited and repeats across integrations, and a classification list that names it can
     * stop matching, or match another send, after an edit.
     * Route creation fails unless the emergency flag allows it with a warning.
     */
    private void checkStepId(NamedNode definition, EndpointSend endpointSend) {
        if (!(definition instanceof ProcessorDefinition<?> processor) || processor.hasCustomIdAssigned()
                || DefaultStepClassifier.isInternalScheme(endpointSend.endpointUri())) {
            return;
        }
        String routeId = routeId(definition);
        String endpoint = EndpointUriMasking.mask(endpointSend.endpointUri());
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

    private static boolean inDeadLetterRoute(NamedNode definition) {
        return PlatformDeadLetterRoute.ROUTE_ID.equals(routeId(definition));
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

    /**
     * A send in progress: the credential headers it withheld, its own step, which a send
     * it makes in turn, such as a tracked send in a {@code direct:} route, replaces on the
     * exchange, and the Kafka record metadata it found before it sent.
     */
    private record Send(java.util.Map<String, Object> withheld, java.util.Map<String, Object> step,
            TrafficSignals.PublishSnapshot trafficSnapshot) {
        void restoreStep(Exchange exchange) {
            for (String name : DefaultStepTracker.CURRENT_STEP_PROPERTIES) {
                Object value = step.get(name);
                if (value == null) {
                    exchange.removeProperty(name);
                } else {
                    exchange.setProperty(name, value);
                }
            }
        }
    }

    /** Fails the exchange. An exchange that already failed keeps its exception unchanged, since it may be shared. */
    private static void markFailed(Exchange exchange, Throwable failure) {
        Exception current = exchange.getException();
        if (current == null) {
            exchange.setException(failure);
        } else if (current != failure) {
            LOG.warn("Completing a send of exchange {} failed after the send itself had failed",
                    exchange.getExchangeId(), failure);
        }
    }

    static class StepTrackingProcessor implements AsyncProcessor {
        private final AsyncProcessor delegate;
        private final EndpointSend endpointSend;
        private final StepTracker stepTracker;
        private final TrafficSignals trafficSignals;
        private final boolean httpSend;
        private final boolean dynamicScheme;
        private final Endpoint endpoint;
        private final boolean mayPublishToKafka;
        private final String routeId;
        private final PublishedCredentialHeaders publishedCredentials;

        StepTrackingProcessor(AsyncProcessor delegate, EndpointSend endpointSend, StepTracker stepTracker) {
            this(delegate, endpointSend, stepTracker, false);
        }

        StepTrackingProcessor(AsyncProcessor delegate, EndpointSend endpointSend, StepTracker stepTracker,
                boolean dynamicScheme) {
            this(delegate, endpointSend, stepTracker, null, dynamicScheme, null, false, null, null);
        }

        StepTrackingProcessor(AsyncProcessor delegate, EndpointSend endpointSend, StepTracker stepTracker,
                TrafficSignals trafficSignals, boolean dynamicScheme, Endpoint endpoint, boolean mayPublishToKafka,
                String routeId, PublishedCredentialHeaders publishedCredentials) {
            this.delegate = delegate;
            this.endpointSend = endpointSend;
            this.stepTracker = stepTracker;
            this.trafficSignals = trafficSignals;
            this.httpSend = ForwardedCredentialHeaders.isHttp(endpointSend.endpointUri());
            this.dynamicScheme = dynamicScheme;
            this.endpoint = endpoint;
            this.mayPublishToKafka = mayPublishToKafka;
            this.routeId = routeId;
            this.publishedCredentials = publishedCredentials;
        }

        /** Completes through {@link #process(Exchange, AsyncCallback)} and waits, with any failure on the exchange. */
        @Override
        public void process(Exchange exchange) throws Exception {
            PluginHelper.getAsyncProcessorAwaitManager(exchange.getContext()).process(this, exchange);
        }

        /**
         * Calls {@code callback} exactly once. A step outcome that cannot be checked or
         * recorded, for example during a Message Store outage, fails the exchange and keeps
         * the input from being parked or committed (see {@link StepOutcomeFailure}). A failed
         * send keeps its own exception unchanged.
         */
        @Override
        public boolean process(Exchange exchange, AsyncCallback callback) {
            Send send;
            try {
                send = prepare(exchange);
            } catch (Throwable failure) {
                markFailed(exchange, failure);
                callback.done(true);
                return true;
            }
            if (send == null) {
                callback.done(true);
                return true;
            }

            return delegate.process(exchange, doneSync -> {
                try {
                    completed(exchange, send);
                } finally {
                    callback.done(doneSync);
                }
            });
        }

        /** Prepares the send, or returns {@code null} when an earlier attempt delivered this step. */
        private Send prepare(Exchange exchange) {
            Throwable earlier = StepOutcomeFailure.of(exchange);
            if (earlier != null) {
                // A store failure ends the attempt: no later send of it, nor a redelivery of this
                // one, is made, since nothing it does can be committed.
                exchange.getExchangeExtension().setRedeliveryExhausted(true);
                throw new StepOutcomeStoreException("Not sent: a step outcome of this attempt was not checked or "
                        + "recorded, so the input is read again", earlier);
            }
            stepOutcome(exchange, () -> stepTracker.beforeSend(exchangeWithInterceptProperties(exchange)));
            if (alreadyDelivered(exchange)) {
                stepTracker.restoreSkippedOutput(exchange);
                stepTracker.recordSkipped(exchange);
                return null;
            }
            return new Send(withholdCredentials(exchange), currentStep(exchange), beforePublish(exchange));
        }

        private static java.util.Map<String, Object> currentStep(Exchange exchange) {
            java.util.Map<String, Object> step = new java.util.HashMap<>();
            for (String name : DefaultStepTracker.CURRENT_STEP_PROPERTIES) {
                Object value = exchange.getProperty(name);
                if (value != null) {
                    step.put(name, value);
                }
            }
            return step;
        }

        /**
         * Records the send's outcome. Never throws: a failure fails the exchange. A send that
         * ran another tracked send, for example through a {@code direct:} route, records its
         * own step, not the one the inner send left on the exchange: the inner send recorded
         * that one already, and an error handler in the inner route may have handled its
         * failure.
         */
        private void completed(Exchange exchange, Send send) {
            try {
                warnAboutPublishedCredentials(exchange);
                ForwardedCredentialHeaders.restore(exchange, send.withheld());
                afterPublish(send.trafficSnapshot(), exchange, exchange.getException() == null);
            } catch (Throwable failure) {
                markFailed(exchange, failure);
            }
            try {
                send.restoreStep(exchange);
                if (exchange.getException() == null) {
                    stepOutcome(exchange, () -> stepTracker.recordDelivered(exchange));
                } else {
                    stepOutcome(exchange, () -> stepTracker.recordFailed(exchange));
                }
            } catch (Throwable failure) {
                markFailed(exchange, failure);
            }
        }

        /** Checks or records a step outcome; a failure also keeps the input from being parked or committed. */
        private static void stepOutcome(Exchange exchange, Runnable work) {
            try {
                work.run();
            } catch (RuntimeException | Error failure) {
                StepOutcomeFailure.record(exchange, failure);
                // No redelivery waits: a redelivery of this send fails before it sends.
                exchange.getExchangeExtension().setRedeliveryExhausted(true);
                throw failure;
            }
        }

        private Exchange exchangeWithInterceptProperties(Exchange exchange) {
            exchange.setProperty(Exchange.INTERCEPTED_ENDPOINT, endpointSend.endpointUri());
            exchange.setProperty(Exchange.INTERCEPTED_NODE_ID, endpointSend.nodeId());
            return exchange;
        }

        /** Completes through {@link #process(Exchange, AsyncCallback)}, with any failure on the exchange. */
        @Override
        public java.util.concurrent.CompletableFuture<Exchange> processAsync(Exchange exchange) {
            var callback = new AsyncCallbackToCompletableFutureAdapter<>(exchange);
            process(exchange, callback);
            return callback.getFuture();
        }

        /** After the send and before withheld headers return, so the exchange shows what was published. */
        private void warnAboutPublishedCredentials(Exchange exchange) {
            if (mayPublishToKafka && publishedCredentials != null && exchange.getException() == null) {
                publishedCredentials.check(exchange, endpoint, routeId, endpointSend.nodeId());
            }
        }

        private java.util.Map<String, Object> withholdCredentials(Exchange exchange) {
            // A send that picks its component at send time may be http, so it is treated as one.
            return httpSend || dynamicScheme
                    ? ForwardedCredentialHeaders.withhold(exchange, endpointSend.nodeId()) : java.util.Map.of();
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
