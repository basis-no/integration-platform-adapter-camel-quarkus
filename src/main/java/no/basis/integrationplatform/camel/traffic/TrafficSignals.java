package no.basis.integrationplatform.camel.traffic;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import no.basis.integrationplatform.camel.KafkaRecordHeaders;
import org.apache.camel.component.kafka.KafkaEndpoint;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Optional process-local activity export through an existing Micrometer registry.
 * Signals measure Kafka activity, never unique business transactions or delivery
 * completion. Telemetry failures cannot fail a message, publish or commit.
 */
@ApplicationScoped
public class TrafficSignals {
    static final long MAX_EXACT = (1L << 53) - 1;
    private static final Logger LOG = LoggerFactory.getLogger(TrafficSignals.class);
    enum Signal { record_admitted, record_published }
    private final Map<Signal, Count> counts = new EnumMap<>(Signal.class);
    private final Set<String> inputTopics;
    private final AtomicInteger producerEndpoints = new AtomicInteger();
    private final AtomicInteger kafkaBoundaries = new AtomicInteger();
    private volatile boolean registryReady;

    /**
     * Creates the optional exporter; an absent or ambiguous registry is unsupported.
     *
     * @param registries existing application metric registries
     * @param selected comma-separated fixed activity signal names; empty disables
     * @param topics exact producer destination input topics; no wildcard matching
     */
    @Inject
    public TrafficSignals(Instance<MeterRegistry> registries,
            @ConfigProperty(name = "basis.traffic.signals") Optional<String> selected,
            @ConfigProperty(name = "basis.traffic.input-topics") Optional<String> topics) {
        this(resolveRegistry(registries), selected.orElse(""), topics.orElse(""));
    }

    TrafficSignals(MeterRegistry registry, String selected, String topics) {
        Set<String> names = split(selected);
        boolean validNames = names.stream().allMatch(n -> n.equals("record_admitted") || n.equals("record_published"));
        for (Signal signal : Signal.values()) counts.put(signal, new Count(validNames && names.contains(signal.name())));
        if (!validNames) LOG.warn("Activity signal selection is invalid; activity coverage is unknown");
        inputTopics = split(topics);
        if (inputTopics.stream().anyMatch(t -> !t.matches("[A-Za-z0-9._-]{1,249}") || t.equals(".") || t.equals("..")))
            invalidate(Signal.record_published);
        if (inputTopics.isEmpty()) invalidate(Signal.record_published);
        if (registry == null || names.isEmpty()) return;
        try {
            for (Signal signal : Signal.values()) {
                Count count = counts.get(signal);
                FunctionCounter.builder("integration_traffic_events", count, c -> c.total.get())
                        .tag("signal", signal.name()).register(registry);
                Gauge.builder("integration_traffic_signal_supported", this, self -> self.supported(signal) ? 1 : 0)
                        .tag("signal", signal.name()).register(registry);
            }
            double started = ManagementFactory.getRuntimeMXBean().getStartTime() / 1000.0;
            Gauge.builder("integration_traffic_process_start_time_seconds", () -> started).register(registry);
            registryReady = true;
        } catch (RuntimeException unavailable) {
            for (Count count : counts.values()) count.invalid.set(true);
            LOG.warn("Activity metrics could not be registered; activity coverage is unknown");
        }
    }

    private static MeterRegistry resolveRegistry(Instance<MeterRegistry> registries) {
        try { return registries.isResolvable() ? registries.get() : null; }
        catch (RuntimeException unavailable) { return null; }
    }

    private static Set<String> split(String values) {
        Set<String> result = new HashSet<>();
        if (!values.isBlank()) for (String value : values.split(",", -1)) result.add(value.trim());
        return result;
    }

    boolean supported(Signal signal) {
        Count count = counts.get(signal);
        return registryReady && count.selected && !count.invalid.get()
                && (signal != Signal.record_published || producerEndpoints.get() > 0)
                && (signal != Signal.record_admitted || kafkaBoundaries.get() > 0);
    }

    /**
     * Registers a selected Kafka input boundary during route preparation. A
     * runtime without one cannot report supported zero admission activity.
     */
    public void observeKafkaBoundary() {
        kafkaBoundaries.incrementAndGet();
    }

    /**
     * Records one admitted Kafka record after boundary identity/payload admission.
     * A missing concrete record coordinate invalidates coverage instead of counting
     * an empty poll, timer tick or synthetic error-handler exchange.
     *
     * @param exchange actual Kafka input exchange
     */
    public void recordAdmitted(Exchange exchange) {
        if (!supported(Signal.record_admitted)) return;
        try {
            Object topic = KafkaRecordHeaders.topic(exchange.getMessage());
            Object offset = KafkaRecordHeaders.offset(exchange.getMessage());
            Object partition = KafkaRecordHeaders.partition(exchange.getMessage());
            if (!(topic instanceof String name) || name.isBlank()
                    || !(offset instanceof Number n) || n.longValue() < 0
                    || !(partition instanceof Number p) || p.intValue() < 0) {
                invalidate(Signal.record_admitted); return;
            }
            increment(Signal.record_admitted, 1);
        } catch (RuntimeException unavailable) { invalidate(Signal.record_admitted); }
    }

    /**
     * Validates producer metadata and ACK configuration without altering it.
     * Every observed Kafka producer must support confirmed record metadata; a
     * mixed unsafe producer configuration makes publication coverage unknown.
     *
     * @param endpoint resolved static producer endpoint
     */
    public void observeProducerEndpoint(Endpoint endpoint) {
        if (!counts.get(Signal.record_published).selected || !(endpoint instanceof KafkaEndpoint kafka)) return;
        try {
            var config = kafka.getConfiguration();
            Map<String, Object> additional = config.getAdditionalProperties();
            Object configuredAcks = additional != null && additional.get("acks") != null
                    ? additional.get("acks") : config.getRequestRequiredAcks();
            String acks = String.valueOf(configuredAcks);
            boolean transactional = config.isTransacted()
                    || (config.getTransactionalId() != null && !config.getTransactionalId().isBlank())
                    || (additional != null && additional.get("transactional.id") != null);
            if (transactional || !Boolean.TRUE.equals(config.isRecordMetadata())
                    || !(acks.equals("-1") || acks.equals("all") || acks.equals("1"))) {
                invalidate(Signal.record_published); return;
            }
            producerEndpoints.incrementAndGet();
        } catch (RuntimeException unavailable) { invalidate(Signal.record_published); }
    }

    /**
     * Marks an unresolved dynamic Kafka destination as unsupported rather than
     * infer ACK configuration or completeness from a URI expression.
     *
     * @param uri dynamic producer expression
     */
    public void observeDynamicEndpoint(String uri) {
        if (!counts.get(Signal.record_published).selected) return;
        int colon = uri == null ? -1 : uri.indexOf(':');
        if (colon < 0 || uri.substring(0, colon).contains("${") || uri.startsWith("kafka:"))
            invalidate(Signal.record_published);
    }

    /**
     * Captures metadata object identities before an actual Kafka send, without
     * clearing, modifying or exposing application headers or payloads.
     *
     * @param uri resolved producer URI
     * @param exchange exchange about to be sent
     * @return per-send snapshot, or null when publication export is unsupported
     */
    public PublishSnapshot beforePublish(String uri, Exchange exchange) {
        if (!supported(Signal.record_published) || uri == null || !uri.startsWith("kafka:")) return null;
        try {
            return new PublishSnapshot(metadata(exchange));
        } catch (RuntimeException unavailable) { invalidate(Signal.record_published); return null; }
    }

    /**
     * Counts newly ACK-confirmed records only after producer completion. Valid
     * metadata from a partially failed batch still counts; a failed send without
     * any ACK counts nothing. Duplicate callbacks and reused headers do not count.
     *
     * @param snapshot pre-send identities
     * @param exchange completed send exchange
     * @param sendSucceeded whether the delegate completed without a send failure
     */
    public void afterPublish(PublishSnapshot snapshot, Exchange exchange, boolean sendSucceeded) {
        if (snapshot == null || !snapshot.finished.compareAndSet(false, true)) return;
        try {
            Set<Object> seen = Collections.newSetFromMap(new IdentityHashMap<>());
            long delta = 0;
            boolean fresh = false;
            for (Object item : metadata(exchange)) {
                if (snapshot.previous.contains(item) || !seen.add(item)) continue;
                fresh = true;
                // Camel includes null/unassigned entries for failed records in
                // the same completed batch. They are not acknowledgements.
                if (!sendSucceeded && (item == null
                        || (item instanceof RecordMetadata failed && !failed.hasOffset()))) continue;
                if (!(item instanceof RecordMetadata record) || !record.hasOffset()) {
                    invalidate(Signal.record_published); return;
                }
                if (inputTopics.contains(record.topic())) delta++;
            }
            if (!fresh && sendSucceeded) { invalidate(Signal.record_published); return; }
            increment(Signal.record_published, delta);
        } catch (RuntimeException unavailable) { invalidate(Signal.record_published); }
    }

    private static List<?> metadata(Exchange exchange) {
        Object value = KafkaRecordHeaders.recordMetadata(exchange.getMessage());
        if (value == null) return List.of();
        if (value instanceof List<?> list) return list;
        throw new IllegalArgumentException("Kafka record metadata is not a list");
    }

    void increment(Signal signal, long delta) {
        if (!supported(signal) || delta == 0) return;
        Count count = counts.get(signal);
        for (;;) {
            long previous = count.total.get();
            if (delta < 0 || delta > MAX_EXACT - previous) { invalidate(signal); return; }
            if (count.total.compareAndSet(previous, previous + delta)) return;
        }
    }

    private void invalidate(Signal signal) {
        Count count = counts.get(signal);
        if (count.invalid.compareAndSet(false, true) && count.selected)
            LOG.warn("Activity signal {} is unsupported; activity coverage is unknown", signal);
    }

    private static final class Count {
        final boolean selected;
        final AtomicLong total = new AtomicLong();
        final AtomicBoolean invalid = new AtomicBoolean();
        Count(boolean selected) { this.selected = selected; }
    }

    /** Opaque per-send bookkeeping; it contains no business values. */
    public static final class PublishSnapshot {
        private final Set<Object> previous = Collections.newSetFromMap(new IdentityHashMap<>());
        private final AtomicBoolean finished = new AtomicBoolean();
        private PublishSnapshot(List<?> records) { previous.addAll(records); }
    }
}
