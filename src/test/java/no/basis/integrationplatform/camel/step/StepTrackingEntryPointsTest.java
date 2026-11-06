package no.basis.integrationplatform.camel.step;

import no.basis.integrationplatform.camel.ReceiverComponent;
import no.basis.integrationplatform.camel.model.StepResult;
import org.apache.camel.AsyncProcessor;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.model.ToDefinition;
import org.apache.camel.processor.SendProcessor;
import org.apache.camel.support.DefaultExchange;
import org.apache.camel.support.service.ServiceHelper;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

/** Every entry point of a tracked send completes the same way when its outcome cannot be recorded. */
class StepTrackingEntryPointsTest {
    @ParameterizedTest
    @ValueSource(strings = {"sync", "async-future"})
    void aFailedSendKeepsItsExceptionWhenItsOutcomeCannotBeRecorded(String entryPoint) throws Exception {
        var rejection = new IllegalArgumentException("synthetic receiver rejection");
        try (var context = new DefaultCamelContext()) {
            context.addComponent("receiver", new ReceiverComponent(e -> {
                throw rejection;
            }));
            context.start();
            var target = new SendProcessor(context.getEndpoint("receiver:orders"));
            ServiceHelper.startService(target);
            var definition = new ToDefinition("receiver:orders");
            definition.setId("deliver-order");
            var wrapped = (AsyncProcessor) new StepTrackingInterceptStrategy(new DefaultStepTracker(new StepOutcomeStore() {
                public boolean wasDelivered(String idempotencyKey) { return false; }
                public void recordDelivered(StepResult step) { throw outage(); }
                public void recordFailed(StepResult step) { throw outage(); }
            }, new DefaultStepClassifier("", "", "deliver-order"))).wrapProcessorInInterceptors(context, definition, target, null);
            Exchange exchange = new DefaultExchange(context);
            StepOutcomeFailure.begin(exchange);

            if (entryPoint.equals("sync")) {
                wrapped.process(exchange);
            } else {
                assertSame(exchange, wrapped.processAsync(exchange).get(10, TimeUnit.SECONDS));
            }

            assertSame(rejection, exchange.getException());
            assertEquals(0, rejection.getSuppressed().length, "The receiver's exception may be shared, so it is not changed");
            assertInstanceOf(StepOutcomeStoreException.class, StepOutcomeFailure.of(exchange));
            ServiceHelper.stopService(target);
        }
    }

    private static StepOutcomeStoreException outage() {
        return new StepOutcomeStoreException("synthetic step outcome outage", null);
    }
}
