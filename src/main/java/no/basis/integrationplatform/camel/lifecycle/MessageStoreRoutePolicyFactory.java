package no.basis.integrationplatform.camel.lifecycle;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import no.basis.integrationplatform.camel.store.MessageStoreSink;
import org.apache.camel.CamelContext;
import org.apache.camel.NamedNode;
import org.apache.camel.model.RouteDefinition;
import org.apache.camel.spi.RoutePolicy;
import org.apache.camel.spi.RoutePolicyFactory;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class MessageStoreRoutePolicyFactory implements RoutePolicyFactory {
    private final MessageStoreSink sink;
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
        // Only a Kafka input has the v1 parking, commit and replay contract.
        // Internal direct/HTTP exchanges may contain transformed receiver data.
        if (!(route instanceof RouteDefinition definition) || definition.getInput() == null) {
            return null;
        }
        String uri = definition.getInput().getEndpointUri();
        if (uri == null) {
            return null;
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
            return null;
        }
        return new MessageStoreRoutePolicy(sink, integrationName, payloadPolicy);
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
