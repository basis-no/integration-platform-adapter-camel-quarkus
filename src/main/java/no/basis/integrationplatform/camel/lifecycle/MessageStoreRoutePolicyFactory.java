package no.basis.integrationplatform.camel.lifecycle;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import no.basis.integrationplatform.camel.store.MessageStoreSink;
import no.basis.integrationplatform.camel.traffic.TrafficSignals;
import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.NamedNode;
import org.apache.camel.component.kafka.KafkaEndpoint;
import org.apache.camel.model.RouteDefinition;
import org.apache.camel.spi.RoutePolicy;
import org.apache.camel.spi.RoutePolicyFactory;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class MessageStoreRoutePolicyFactory implements RoutePolicyFactory {
    private static final Logger LOG = LoggerFactory.getLogger(MessageStoreRoutePolicyFactory.class);
    private final MessageStoreSink sink;
    @Inject TrafficSignals trafficSignals;
    private final String integrationName;
    private final PayloadCapturePolicy payloadPolicy;
    private final Set<String> auditRoutes;
    private final Set<String> seenAuditRoutes = new LinkedHashSet<>();
    private final Set<String> inputAuditRoutes;
    private final Set<String> seenInputAuditRoutes = new LinkedHashSet<>();

    @Inject
    public MessageStoreRoutePolicyFactory(
            MessageStoreSink sink,
            @ConfigProperty(name = "app.name", defaultValue = "unknown-integration") String integrationName,
            @ConfigProperty(name = "basis.message-store.payload.mode", defaultValue = "postgres-inline") String payloadMode,
            @ConfigProperty(name = "basis.message-store.payload.max-bytes",
                    defaultValue = "" + PayloadCapturePolicy.DEFAULT_MAX_BYTES) long payloadMaxBytes,
            @ConfigProperty(name = "basis.message-store.payload.retention-days", defaultValue = "30") long payloadRetentionDays,
            @ConfigProperty(name = "basis.message-store.replay.enabled", defaultValue = "true") boolean replayEnabled,
            @ConfigProperty(name = "basis.delivery-audit.routes") Optional<String> auditRouteSelection,
            @ConfigProperty(name = "basis.input-audit.routes") Optional<String> inputAuditRouteSelection) {
        this.sink = sink;
        this.integrationName = integrationName;
        this.payloadPolicy = PayloadCapturePolicy.fromConfig(payloadMode, payloadMaxBytes, payloadRetentionDays, replayEnabled);
        this.auditRoutes = parseRoutes(auditRouteSelection, "basis.delivery-audit.routes");
        this.inputAuditRoutes = parseRoutes(inputAuditRouteSelection, "basis.input-audit.routes");
        Set<String> overlap = new LinkedHashSet<>(auditRoutes);
        overlap.retainAll(inputAuditRoutes);
        if (!overlap.isEmpty()) {
            throw new IllegalArgumentException("A route cannot be both input inspection and delivery audit: "
                    + String.join(", ", overlap));
        }
    }

    private static Set<String> parseRoutes(Optional<String> selection, String property) {
        Set<String> routes = new LinkedHashSet<>();
        String selected = selection.orElse("");
        if (!selected.isBlank()) {
            for (String value : selected.split(",", -1)) {
                String id = value.trim();
                if (id.isEmpty() || id.contains("*") || id.contains("?")) {
                    throw new IllegalArgumentException(property + " requires exact, non-empty route ids");
                }
                routes.add(id);
            }
        }
        return routes;
    }

    public MessageStoreRoutePolicyFactory(MessageStoreSink sink, String integrationName, String payloadMode,
            long payloadMaxBytes, long payloadRetentionDays, boolean replayEnabled, Optional<String> auditRoutes) {
        this(sink, integrationName, payloadMode, payloadMaxBytes, payloadRetentionDays, replayEnabled,
                auditRoutes, Optional.empty());
    }

    public MessageStoreRoutePolicyFactory(MessageStoreSink sink, String integrationName, String payloadMode,
            long payloadMaxBytes, long payloadRetentionDays, boolean replayEnabled, String auditRoutes,
            String inputAuditRoutes) {
        this(sink, integrationName, payloadMode, payloadMaxBytes, payloadRetentionDays, replayEnabled,
                Optional.ofNullable(auditRoutes), Optional.ofNullable(inputAuditRoutes));
    }

    public MessageStoreRoutePolicyFactory(MessageStoreSink sink, String integrationName, String payloadMode,
            long payloadMaxBytes, long payloadRetentionDays, boolean replayEnabled, String auditRoutes) {
        this(sink, integrationName, payloadMode, payloadMaxBytes, payloadRetentionDays, replayEnabled,
                Optional.ofNullable(auditRoutes));
    }

    public MessageStoreRoutePolicyFactory(MessageStoreSink sink, String integrationName, String payloadMode,
            long payloadMaxBytes, long payloadRetentionDays, boolean replayEnabled) {
        this(sink, integrationName, payloadMode, payloadMaxBytes, payloadRetentionDays, replayEnabled, "");
    }

    @Override
    public RoutePolicy createRoutePolicy(CamelContext camelContext, String routeId, NamedNode route) {
        // Only a Kafka input has the adapter's parking, commit and replay behavior.
        // Internal direct/HTTP exchanges may contain transformed receiver data.
        // Every other route still reads a replay's redo list, so none of its sends carries it.
        if (!(route instanceof RouteDefinition definition) || definition.getInput() == null) {
            return new RedoStepsRoutePolicy();
        }
        String uri = definition.getInput().getEndpointUri();
        if (uri == null) {
            return new RedoStepsRoutePolicy();
        }
        uri = camelContext.resolvePropertyPlaceholders(uri);
        if (inputAuditRoutes.contains(routeId)) {
            if (uri.startsWith("kafka:") || routeId.equals("bip-confirmed-dead-letter")) {
                throw new IllegalArgumentException("Input inspection route '" + routeId
                        + "' must be a non-Kafka application input; managed Kafka recovery remains unchanged");
            }
            seenInputAuditRoutes.add(routeId);
            return new MessageStoreInputAuditRoutePolicy(sink, integrationName, payloadPolicy);
        }
        if (auditRoutes.contains(routeId)) {
            if (!uri.startsWith("direct:")) {
                throw new IllegalArgumentException("Audit route '" + routeId + "' must consume a dedicated direct: request");
            }
            seenAuditRoutes.add(routeId);
            return new MessageStoreAuditRoutePolicy(sink, integrationName, payloadPolicy);
        }
        if (!uri.startsWith("kafka:")) {
            return new RedoStepsRoutePolicy();
        }
        warnAboutKafkaOffsetReset(routeId, camelContext, uri);
        MessageStoreRoutePolicy policy = new MessageStoreRoutePolicy(sink, integrationName, payloadPolicy);
        policy.setTrafficSignals(trafficSignals);
        if (trafficSignals != null) trafficSignals.observeKafkaBoundary();
        return policy;
    }

    /**
     * A record that is not committed is read again after Camel's reconnect, which resumes from
     * the committed offset. On a partition without one, the consumer resumes from its
     * {@code auto.offset.reset}; anything but {@code earliest} skips the record ({@code latest})
     * or stops the consumer ({@code none}). A consumer without a group id gets a new generated
     * group on every start, which has no committed offsets. In 0.3.x both are warnings, not a
     * refusal to start.
     */
    static void warnAboutKafkaOffsetReset(String routeId, CamelContext camelContext, String uri) {
        Endpoint endpoint;
        java.util.Properties consumer;
        String group;
        try {
            endpoint = camelContext.getEndpoint(uri);
            if (!(endpoint instanceof KafkaEndpoint kafka)) {
                return;
            }
            // The consumer's effective values: the endpoint option, the component default, or an
            // additionalProperties entry, which Camel applies last.
            consumer = kafka.getConfiguration().createConsumerProperties();
            // Camel's consumer sets group.id from groupId, or a random one, over any
            // additionalProperties.group.id, so only groupId counts.
            group = kafka.getConfiguration().getGroupId();
        } catch (RuntimeException unreadable) {
            // The route fails on its own when Camel creates its consumer; this check only warns.
            LOG.debug("Could not read the Kafka consumer settings of route {}", routeId, unreadable);
            return;
        }
        Object effective = consumer.get("auto.offset.reset");
        String value = effective == null ? null : effective.toString().trim();
        if (value == null || !"earliest".equals(value.toLowerCase(Locale.ROOT))) {
            LOG.warn("Kafka input route '{}' ({}) uses auto.offset.reset={}. A record that is not committed is read "
                            + "again from the committed offset; on a partition without one, any value other than "
                            + "earliest skips the record or stops the consumer. Remove the autoOffsetReset or "
                            + "additionalProperties.auto.offset.reset override from the endpoint and from "
                            + "camel.component.kafka.auto-offset-reset, or set it to earliest",
                    routeId, endpoint.getEndpointBaseUri(), value == null ? "latest (Kafka's default)" : value);
        }
        if (group == null || group.isBlank()) {
            LOG.warn("Kafka input route '{}' ({}) has no groupId, so its consumer gets a new consumer group on every "
                            + "start, without committed offsets: every start begins where auto.offset.reset says, "
                            + "and with earliest every retained record of its partitions is read again. Set a stable "
                            + "groupId on the endpoint or camel.component.kafka.group-id; "
                            + "additionalProperties.group.id is not used",
                    routeId, endpoint.getEndpointBaseUri());
        }
    }

    void validateAuditRoutes(@Observes StartupEvent ignored) {
        Set<String> missing = new LinkedHashSet<>(auditRoutes);
        missing.removeAll(seenAuditRoutes);
        if (!missing.isEmpty()) {
            throw new IllegalArgumentException("basis.delivery-audit.routes names unknown routes: " + String.join(", ", missing));
        }
        missing = new LinkedHashSet<>(inputAuditRoutes);
        missing.removeAll(seenInputAuditRoutes);
        if (!missing.isEmpty()) {
            throw new IllegalArgumentException("basis.input-audit.routes names unknown routes: " + String.join(", ", missing));
        }
    }
}
