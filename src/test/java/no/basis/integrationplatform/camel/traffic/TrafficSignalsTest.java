package no.basis.integrationplatform.camel.traffic;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.KafkaEndpoint;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class TrafficSignalsTest {
    private final DefaultCamelContext context = new DefaultCamelContext();
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    @AfterEach void close() throws Exception { registry.close(); context.close(); }

    @Test void disabledSignalsDoNotRegisterOrInspectPayloads() {
        var signals = new TrafficSignals(registry, "", "");
        var exchange = new DefaultExchange(context);
        assertDoesNotThrow(() -> signals.recordAdmitted(exchange));
        assertNull(signals.beforePublish("kafka:input", exchange));
        assertNull(registry.find("integration_traffic_events").functionCounter());
    }
    @Test void missingRegistryIsUnsupportedRatherThanMessageFailure() {
        var signals = new TrafficSignals(null, "record_admitted", "");
        assertFalse(signals.supported(TrafficSignals.Signal.record_admitted));
        assertDoesNotThrow(() -> signals.recordAdmitted(new DefaultExchange(context)));
    }
    @Test void noKafkaBoundaryIsUnknownAndUnselectedSignalDoesNotAffectAdmission() {
        var signals = new TrafficSignals(registry, "record_admitted", "");
        assertEquals(0, supported("record_admitted"));
        signals.observeKafkaBoundary();
        assertEquals(1, supported("record_admitted"));
        assertEquals(0, supported("record_published"));
    }
    @Test void invalidSelectionCannotBecomeKnownZero() {
        var signals = new TrafficSignals(registry, "record_admitted,typo", "");
        signals.observeKafkaBoundary();
        assertEquals(0, supported("record_admitted"));
    }
    @Test void realRecordCoordinatesCountAdmissionIncludingRedelivery() {
        var signals = new TrafficSignals(registry, "record_admitted", "");
        signals.observeKafkaBoundary();
        var exchange = new DefaultExchange(context);
        exchange.getMessage().setHeader(KafkaConstants.TOPIC, "input");
        exchange.getMessage().setHeader(KafkaConstants.PARTITION, 0);
        exchange.getMessage().setHeader(KafkaConstants.OFFSET, 7L);
        signals.recordAdmitted(exchange); signals.recordAdmitted(exchange);
        assertEquals(2, total("record_admitted"));
        assertEquals(1, supported("record_admitted"));
    }
    @Test void laterCamelHeaderNamesCountAdmission() {
        // camel-kafka after 4.18.0 names the record coordinates CamelKafka*.
        var signals = new TrafficSignals(registry, "record_admitted", "");
        signals.observeKafkaBoundary();
        var exchange = new DefaultExchange(context);
        exchange.getMessage().setHeader("CamelKafkaTopic", "input");
        exchange.getMessage().setHeader("CamelKafkaPartition", 0);
        exchange.getMessage().setHeader("CamelKafkaOffset", 7L);
        signals.recordAdmitted(exchange);
        assertEquals(1, total("record_admitted"));
        assertEquals(1, supported("record_admitted"));
    }
    @Test void laterCamelHeaderNameCountsPublication() {
        var signals = new TrafficSignals(registry, "record_published", "input");
        signals.observeProducerEndpoint(producer(true, "all"));
        var exchange = new DefaultExchange(context);
        var snapshot = signals.beforePublish("kafka:input", exchange);
        exchange.getMessage().setHeader("CamelKafkaRecordMeta", List.of(metadata("input", 1)));
        signals.afterPublish(snapshot, exchange, true);
        assertEquals(1, total("record_published"));
        assertEquals(1, supported("record_published"));
    }
    @Test void timerOrEmptyPollCannotBecomeKnownZeroActivity() {
        var signals = new TrafficSignals(registry, "record_admitted", "");
        signals.observeKafkaBoundary();
        signals.recordAdmitted(new DefaultExchange(context));
        assertEquals(0, total("record_admitted"));
        assertEquals(0, supported("record_admitted"));
    }
    @Test void publicationNeedsExplicitTopicsAndObservedSafeProducer() {
        var signals = new TrafficSignals(registry, "record_published", "input");
        assertEquals(0, supported("record_published"));
        signals.observeProducerEndpoint(producer(true, "all"));
        assertEquals(1, supported("record_published"));
        assertEquals(0, total("record_published"));
        assertNotNull(registry.find("integration_traffic_process_start_time_seconds").gauge());
    }
    @Test void disabledMetadataInvalidatesSelectedCoverage() {
        var signals = publishingSignals();
        signals.observeProducerEndpoint(producer(false, "all"));
        assertEquals(0, supported("record_published"));
    }
    @Test void acksZeroInvalidatesSelectedCoverageIndependently() {
        var signals = publishingSignals();
        signals.observeProducerEndpoint(producer(true, "0"));
        assertEquals(0, supported("record_published"));
    }
    @Test void topicWildcardsCannotBecomeKnownZero() {
        var signals = new TrafficSignals(registry, "record_published", "input-*");
        signals.observeProducerEndpoint(producer(true, "all"));
        assertEquals(0, supported("record_published"));
    }
    @Test void effectiveAcksOverrideIsValidated() {
        var signals = new TrafficSignals(registry, "record_published", "input");
        var endpoint = producer(true, "all");
        endpoint.getConfiguration().setAdditionalProperties(java.util.Map.of("acks", "0"));
        signals.observeProducerEndpoint(endpoint);
        assertEquals(0, supported("record_published"));
    }
    @Test void transactionalAcknowledgementIsNotCommittedPublication() {
        var signals = new TrafficSignals(registry, "record_published", "input");
        var endpoint = producer(true, "all");
        endpoint.getConfiguration().setTransacted(true);
        signals.observeProducerEndpoint(endpoint);
        assertEquals(0, supported("record_published"));
    }
    @Test void onlyFreshAssignedMetadataCountsAfterCompletionOnce() {
        var signals = publishingSignals();
        var exchange = new DefaultExchange(context);
        RecordMetadata old = metadata("input", 0);
        var records = new ArrayList<RecordMetadata>(); records.add(old);
        exchange.getMessage().setHeader(KafkaConstants.KAFKA_RECORD_META, records);
        var snapshot = signals.beforePublish("kafka:input", exchange);
        assertEquals(0, total("record_published"));
        RecordMetadata fresh = metadata("input", 1);
        records.add(fresh); records.add(fresh); records.add(metadata("other", 2));
        signals.afterPublish(snapshot, exchange, true);
        signals.afterPublish(snapshot, exchange, true);
        assertSame(records, exchange.getMessage().getHeader(KafkaConstants.KAFKA_RECORD_META));
        assertEquals(1, total("record_published"));
    }
    @Test void reusedMetadataIsNotAnotherAckAndInvalidatesSuccessCoverage() {
        var signals = publishingSignals();
        var exchange = new DefaultExchange(context);
        exchange.getMessage().setHeader(KafkaConstants.KAFKA_RECORD_META, List.of(metadata("input", 1)));
        var snapshot = signals.beforePublish("kafka:input", exchange);
        signals.afterPublish(snapshot, exchange, true);
        assertEquals(0, total("record_published"));
        assertEquals(0, supported("record_published"));
    }
    @Test void confirmedSubsetOfFailedBatchCountsButNoAckFailureDoesNot() {
        var signals = publishingSignals(); var exchange = new DefaultExchange(context);
        var snapshot = signals.beforePublish("kafka:input", exchange);
        exchange.getMessage().setHeader(KafkaConstants.KAFKA_RECORD_META, java.util.Arrays.asList(metadata("input", 1), null, metadata("input", -1), metadata("input", 2)));
        signals.afterPublish(snapshot, exchange, false);
        assertEquals(2, total("record_published"));
        snapshot = signals.beforePublish("kafka:input", exchange);
        signals.afterPublish(snapshot, exchange, false);
        assertEquals(2, total("record_published")); assertEquals(1, supported("record_published"));
    }
    @Test void unassignedAckMetadataCannotBeReportedAsTraffic() {
        var signals = publishingSignals(); var exchange = new DefaultExchange(context);
        var snapshot = signals.beforePublish("kafka:input", exchange);
        exchange.getMessage().setHeader(KafkaConstants.KAFKA_RECORD_META, List.of(metadata("input", -1)));
        signals.afterPublish(snapshot, exchange, true);
        assertEquals(0, total("record_published")); assertEquals(0, supported("record_published"));
    }
    @Test void dynamicKafkaConfigurationIsUnknownWithoutChangingRoutes() {
        var signals = publishingSignals();
        signals.observeDynamicEndpoint("kafka:${header.topic}");
        assertEquals(0, supported("record_published"));
    }
    @Test void precisionBoundaryInvalidatesBeforeRoundingOrWrapping() {
        var signals = new TrafficSignals(registry, "record_admitted", "");
        signals.observeKafkaBoundary();
        signals.increment(TrafficSignals.Signal.record_admitted, TrafficSignals.MAX_EXACT);
        assertEquals((double) TrafficSignals.MAX_EXACT, total("record_admitted"));
        signals.increment(TrafficSignals.Signal.record_admitted, 1);
        assertEquals((double) TrafficSignals.MAX_EXACT, total("record_admitted"));
        assertEquals(0, supported("record_admitted"));
    }
    @Test void actualSendWrapperWaitsForAsyncCallbackBeforeCounting() throws Exception {
        var signals = new TrafficSignals(registry, "record_published", "input");
        var pending = new java.util.concurrent.atomic.AtomicReference<org.apache.camel.AsyncCallback>();
        var finished = new java.util.concurrent.atomic.AtomicInteger();
        var tracker = new no.basis.integrationplatform.camel.step.StepTracker() {
            public void beforeSend(org.apache.camel.Exchange exchange) { }
            public void recordDelivered(org.apache.camel.Exchange exchange) { }
            public void recordFailed(org.apache.camel.Exchange exchange) { }
            public void recordFailedAndRethrow(org.apache.camel.Exchange exchange) { }
            public void recordSkipped(org.apache.camel.Exchange exchange) { }
            public void restoreSkippedOutput(org.apache.camel.Exchange exchange) { }
        };
        var endpoint = producer(true, "all");
        var target = new org.apache.camel.processor.SendProcessor(endpoint) {
            @Override public boolean process(org.apache.camel.Exchange exchange, org.apache.camel.AsyncCallback callback) {
                pending.set(callback); return false;
            }
        };
        var definition = new org.apache.camel.model.ToDefinition("kafka:input");
        definition.setId("publish-input");
        var strategy = new no.basis.integrationplatform.camel.step.StepTrackingInterceptStrategy(tracker, signals);
        var wrapped = (org.apache.camel.AsyncProcessor) strategy.wrapProcessorInInterceptors(context, definition, target, null);
        var exchange = new DefaultExchange(context);
        assertFalse(wrapped.process(exchange, ignored -> finished.incrementAndGet()));
        assertEquals(0, total("record_published")); assertEquals(0, finished.get());
        exchange.getMessage().setHeader(KafkaConstants.KAFKA_RECORD_META, List.of(metadata("input", 1)));
        pending.get().done(false);
        assertEquals(1, total("record_published")); assertEquals(1, finished.get());
    }

    private TrafficSignals publishingSignals() {
        var signals = new TrafficSignals(registry, "record_published", "input");
        signals.observeProducerEndpoint(producer(true, "all")); return signals;
    }
    private KafkaEndpoint producer(boolean recordMetadata, String acks) {
        return context.getEndpoint("kafka:input?brokers=localhost:1&recordMetadata=" + recordMetadata
                + "&requestRequiredAcks=" + acks, KafkaEndpoint.class);
    }
    private static RecordMetadata metadata(String topic, long offset) {
        return new RecordMetadata(new TopicPartition(topic, 0), offset, 0, 0L, 0, 0);
    }
    private double total(String signal) {
        return registry.get("integration_traffic_events").tag("signal", signal).functionCounter().count();
    }
    private double supported(String signal) {
        return registry.get("integration_traffic_signal_supported").tag("signal", signal).gauge().value();
    }
}
