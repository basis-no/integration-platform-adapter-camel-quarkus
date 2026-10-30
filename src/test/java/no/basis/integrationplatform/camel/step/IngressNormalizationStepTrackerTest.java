package no.basis.integrationplatform.camel.step;

import no.basis.integrationplatform.camel.PlatformExchangeProperties;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.ingress.IngressMessageIdentity;
import no.basis.integrationplatform.camel.ingress.IngressNormalizationConfig;
import no.basis.integrationplatform.camel.model.StepResult;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IngressNormalizationStepTrackerTest {
    @Test
    void sourceRedeliveryUsesSameKafkaPublishKeyAndSkipsCompletedPublish() {
        RecordingStore store = new RecordingStore();
        IngressMessageIdentity identity = new IngressMessageIdentity(
                new IngressNormalizationConfig("file-to-kafka"), "file-integration");
        DefaultStepTracker tracker = new DefaultStepTracker(
                store, DefaultStepClassifier.defaults(), null, identity);

        Exchange firstDelivery = fileExchange();
        tracker.beforeSend(firstDelivery);
        tracker.recordDelivered(firstDelivery);

        String messageId = firstDelivery.getMessage().getHeader(PlatformHeaders.MESSAGE_ID, String.class);
        String idempotencyKey = firstDelivery.getMessage().getHeader(PlatformHeaders.IDEMPOTENCY_KEY, String.class);
        store.deliveredKeys.add(idempotencyKey);

        Exchange sourceRedelivery = fileExchange();
        tracker.beforeSend(sourceRedelivery);

        assertEquals(messageId, sourceRedelivery.getMessage().getHeader(PlatformHeaders.MESSAGE_ID));
        assertEquals(idempotencyKey, sourceRedelivery.getMessage().getHeader(PlatformHeaders.IDEMPOTENCY_KEY));
        assertTrue(sourceRedelivery.getProperty(PlatformExchangeProperties.STEP_ALREADY_DELIVERED, Boolean.class));
        assertFalse(store.recordedFailures);
    }

    private Exchange fileExchange() {
        Exchange exchange = new DefaultExchange(new DefaultCamelContext());
        exchange.setProperty(Exchange.INTERCEPTED_ROUTE_ID, "file-to-kafka");
        exchange.setProperty(Exchange.INTERCEPTED_ENDPOINT, "kafka:bip.files.in");
        exchange.setProperty(Exchange.INTERCEPTED_NODE_ID, "publish-canonical-message");
        exchange.getMessage().setHeader("CamelFileRelativePath", "incoming/batch-42.json");
        exchange.getMessage().setHeader("CamelFileLength", 512L);
        exchange.getMessage().setHeader("CamelFileLastModified", 1_723_456_789L);
        return exchange;
    }

    private static class RecordingStore implements StepOutcomeStore {
        private final Set<String> deliveredKeys = new HashSet<>();
        private boolean recordedFailures;

        @Override
        public boolean wasDelivered(String idempotencyKey) {
            return deliveredKeys.contains(idempotencyKey);
        }

        @Override
        public void recordDelivered(StepResult step) {
            deliveredKeys.add(step.idempotencyKey());
        }

        @Override
        public void recordFailed(StepResult step) {
            recordedFailures = true;
        }
    }
}
