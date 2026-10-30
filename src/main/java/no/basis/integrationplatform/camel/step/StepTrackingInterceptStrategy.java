package no.basis.integrationplatform.camel.step;

import jakarta.enterprise.context.ApplicationScoped;
import no.basis.integrationplatform.camel.PlatformExchangeProperties;
import org.apache.camel.AsyncCallback;
import org.apache.camel.AsyncProcessor;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.NamedNode;
import org.apache.camel.Processor;
import org.apache.camel.processor.SendDynamicProcessor;
import org.apache.camel.processor.SendProcessor;
import org.apache.camel.spi.InterceptStrategy;
import org.apache.camel.support.AsyncProcessorConverterHelper;

/** Wraps endpoint-send processors with automatic classification and step tracking. */
@ApplicationScoped
public class StepTrackingInterceptStrategy implements InterceptStrategy {
    private final StepTracker stepTracker;

    /**
     * Creates the intercept strategy.
     *
     * @param stepTracker tracker that owns classification and durable outcomes
     */
    public StepTrackingInterceptStrategy(StepTracker stepTracker) {
        this.stepTracker = stepTracker;
    }

    @Override
    public Processor wrapProcessorInInterceptors(
            CamelContext context,
            NamedNode definition,
            Processor target,
            Processor nextTarget) {

        EndpointSend endpointSend = endpointSend(definition, target);
        if (endpointSend == null) {
            return target;
        }

        AsyncProcessor delegate = AsyncProcessorConverterHelper.convert(target);
        return new StepTrackingProcessor(delegate, endpointSend, stepTracker);
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

        StepTrackingProcessor(AsyncProcessor delegate, EndpointSend endpointSend, StepTracker stepTracker) {
            this.delegate = delegate;
            this.endpointSend = endpointSend;
            this.stepTracker = stepTracker;
        }

        @Override
        public void process(Exchange exchange) throws Exception {
            stepTracker.beforeSend(exchangeWithInterceptProperties(exchange));
            if (alreadyDelivered(exchange)) {
                stepTracker.restoreSkippedOutput(exchange);
                stepTracker.recordSkipped(exchange);
                return;
            }
            try {
                delegate.process(exchange);
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

            return delegate.process(exchange, doneSync -> {
                try {
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
            return delegate.processAsync(exchange).whenComplete((result, failure) -> {
                Exchange completedExchange = result != null ? result : exchange;
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

        private boolean alreadyDelivered(Exchange exchange) {
            return Boolean.TRUE.equals(exchange.getProperty(PlatformExchangeProperties.STEP_ALREADY_DELIVERED, Boolean.class));
        }
    }
}
