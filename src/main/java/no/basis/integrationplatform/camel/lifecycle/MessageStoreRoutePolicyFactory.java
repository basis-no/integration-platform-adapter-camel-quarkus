package no.basis.integrationplatform.camel.lifecycle;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import io.quarkus.runtime.StartupEvent;
import no.basis.integrationplatform.camel.MessageFailureMode;
import no.basis.integrationplatform.camel.MessageStoreTestProfileConfig;
import no.basis.integrationplatform.camel.PlatformRouteConfiguration;
import no.basis.integrationplatform.camel.ingress.IngressNormalizationConfig;
import no.basis.integrationplatform.camel.ingress.IngressReplicaSafety;
import no.basis.integrationplatform.camel.store.MessageStoreSink;
import no.basis.integrationplatform.camel.payload.PayloadObjectStore;
import org.apache.camel.CamelContext;
import org.apache.camel.NamedNode;
import org.apache.camel.spi.RoutePolicy;
import org.apache.camel.spi.RoutePolicyFactory;
import org.apache.camel.model.RouteDefinition;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Selects supported boundary routes and creates their lifecycle policies. */
@ApplicationScoped
public class MessageStoreRoutePolicyFactory implements RoutePolicyFactory {
    private final MessageStoreSink sink;
    private final String integrationName;
    private final PayloadCapturePolicy payloadPolicy;
    private final MessageBoundarySelector boundarySelector;
    private final boolean replayEnabled;
    private final MessageFailureMode failureMode;
    private final boolean kafkaCommitEnabled;
    private final PayloadObjectStore objectStore;
    private final IngressNormalizationConfig ingressNormalizationConfig;
    private final IngressReplicaSafety ingressReplicaSafety;
    private final Set<String> seenNormalizationRoutes = ConcurrentHashMap.newKeySet();

    /**
     * Creates the CDI policy factory from adapter configuration and runtime services.
     *
     * @param sink effective terminal outcome sink
     * @param integrationName stable integration name
     * @param payloadMode payload storage mode
     * @param payloadInlineThresholdBytes optional inline threshold
     * @param legacyPayloadMaxBytes deprecated inline-threshold alias
     * @param payloadObjectMaxBytes object payload limit
     * @param payloadRetentionDays payload retention
     * @param replayEnabled whether replay payload capture is enabled
     * @param failureMode terminal failure behavior
     * @param boundaryRoutes boundary route selection
     * @param testProfile active test-profile policy
     * @param objectStore object payload store
     * @param ingressNormalizationConfig selected normalization routes
     * @param ingressReplicaSafety deployment replica safety validator
     */
    @Inject
    public MessageStoreRoutePolicyFactory(
            MessageStoreSink sink,
            @ConfigProperty(name = "app.name", defaultValue = "unknown-integration") String integrationName,
            @ConfigProperty(name = "basis.message-store.payload.mode", defaultValue = "postgres-inline") String payloadMode,
            @ConfigProperty(name = "basis.message-store.payload.inline-threshold-bytes")
                    Optional<Long> payloadInlineThresholdBytes,
            @ConfigProperty(name = "basis.message-store.payload.max-bytes") Optional<Long> legacyPayloadMaxBytes,
            @ConfigProperty(name = "basis.message-store.payload.object-max-bytes",
                    defaultValue = "" + PayloadCapturePolicy.DEFAULT_OBJECT_MAX_BYTES) long payloadObjectMaxBytes,
            @ConfigProperty(name = "basis.message-store.payload.retention-days", defaultValue = "30") long payloadRetentionDays,
            @ConfigProperty(name = "basis.message-store.replay.enabled", defaultValue = "true") boolean replayEnabled,
            @ConfigProperty(name = "basis.message-store.failure-mode", defaultValue = "dlq") String failureMode,
            @ConfigProperty(name = "basis.message-store.boundary-routes",
                    defaultValue = MessageBoundarySelector.DEFAULT_SELECTION) String boundaryRoutes,
            MessageStoreTestProfileConfig testProfile,
            PayloadObjectStore objectStore,
            IngressNormalizationConfig ingressNormalizationConfig,
            IngressReplicaSafety ingressReplicaSafety) {
        this.sink = sink;
        this.integrationName = integrationName;
        long inlineThreshold = resolveInlineThreshold(payloadInlineThresholdBytes, legacyPayloadMaxBytes);
        this.payloadPolicy = PayloadCapturePolicy.fromConfig(
                payloadMode, inlineThreshold, payloadObjectMaxBytes, payloadRetentionDays, replayEnabled);
        this.boundarySelector = MessageBoundarySelector.fromConfig(boundaryRoutes);
        this.replayEnabled = replayEnabled;
        this.failureMode = MessageFailureMode.fromConfig(failureMode);
        this.kafkaCommitEnabled = testProfile.kafkaCommitEnabled();
        this.objectStore = objectStore;
        this.ingressNormalizationConfig = ingressNormalizationConfig;
        this.ingressReplicaSafety = ingressReplicaSafety;
    }

    static long resolveInlineThreshold(Optional<Long> configured, Optional<Long> legacy) {
        return configured.orElseGet(
                () -> legacy.orElse(PayloadCapturePolicy.DEFAULT_INLINE_THRESHOLD_BYTES));
    }

    /**
     * Creates a Kafka-focused factory for embedding and tests.
     *
     * @param sink terminal outcome sink
     * @param integrationName stable integration name
     * @param payloadMode payload storage mode
     * @param payloadMaxBytes inline payload threshold
     * @param payloadRetentionDays payload retention
     * @param replayEnabled whether replay capture is enabled
     * @param failureMode terminal failure behavior
     * @param boundaryRoutes boundary route selection
     */
    public MessageStoreRoutePolicyFactory(
            MessageStoreSink sink,
            String integrationName,
            String payloadMode,
            long payloadMaxBytes,
            long payloadRetentionDays,
            boolean replayEnabled,
            String failureMode,
            String boundaryRoutes) {
        this(sink, integrationName, payloadMode, payloadMaxBytes, payloadRetentionDays,
                replayEnabled, failureMode, boundaryRoutes, IngressNormalizationConfig.disabled());
    }

    MessageStoreRoutePolicyFactory(
            MessageStoreSink sink,
            String integrationName,
            String payloadMode,
            long payloadMaxBytes,
            long payloadRetentionDays,
            boolean replayEnabled,
            String failureMode,
            String boundaryRoutes,
            IngressNormalizationConfig ingressNormalizationConfig) {
        this(
                sink,
                integrationName,
                payloadMode,
                Optional.of(payloadMaxBytes),
                Optional.empty(),
                PayloadCapturePolicy.DEFAULT_OBJECT_MAX_BYTES,
                payloadRetentionDays,
                replayEnabled,
                failureMode,
                boundaryRoutes,
                new MessageStoreTestProfileConfig(MessageStoreTestProfileConfig.DEFAULT_PROFILE, false),
                PayloadObjectStore.disabled(),
                ingressNormalizationConfig,
                IngressReplicaSafety.declaredReplicas(1));
    }

    @Override
    public RoutePolicy createRoutePolicy(CamelContext camelContext, String routeId, NamedNode route) {
        if (ingressNormalizationConfig.includes(routeId)) {
            validateIngressNormalizationRoute(routeId, route);
            seenNormalizationRoutes.add(routeId);
        }
        if (!boundarySelector.includes(routeId, route)) {
            return null;
        }
        MessageBoundaryKind boundaryKind = boundarySelector.kind(route);
        validateBoundary(routeId, boundaryKind);
        return new MessageStoreRoutePolicy(
                sink, integrationName, payloadPolicy, boundaryKind, kafkaCommitEnabled, objectStore);
    }

    void validateConfiguredNormalizationRoutes(@Observes StartupEvent ignored) {
        Set<String> missingRoutes = new java.util.LinkedHashSet<>(ingressNormalizationConfig.routeIds());
        missingRoutes.removeAll(seenNormalizationRoutes);
        if (!missingRoutes.isEmpty()) {
            throw new IllegalArgumentException(IngressNormalizationConfig.PROPERTY
                    + " names route ids that do not exist: " + String.join(", ", missingRoutes));
        }
    }

    private void validateIngressNormalizationRoute(String routeId, NamedNode route) {
        if (boundarySelector.includes(routeId, route)) {
            throw new IllegalArgumentException(
                    "Ingress normalization route '" + routeId
                            + "' must not also be selected by basis.message-store.boundary-routes; "
                            + "the downstream Kafka consumer is the lifecycle and replay boundary");
        }
        if (!(route instanceof RouteDefinition definition)
                || !usesRouteConfiguration(definition, PlatformRouteConfiguration.INGRESS_NORMALIZATION)) {
            throw new IllegalArgumentException(
                    "Ingress normalization route '" + routeId + "' must use routeConfigurationId="
                            + PlatformRouteConfiguration.INGRESS_NORMALIZATION
                            + " so source failures propagate without platform DLQ parking");
        }
        ingressReplicaSafety.validate(routeId, definition);
        if (boundarySelector.kind(route) == MessageBoundaryKind.HTTP) {
            throw new IllegalArgumentException(
                    "Synchronous HTTP route '" + routeId + "' cannot use the ingress normalization profile; "
                            + "use an explicit audit-only HTTP boundary or accept asynchronously through Kafka");
        }
    }

    private boolean usesRouteConfiguration(RouteDefinition definition, String requiredId) {
        Set<String> appliedIds = definition.getAppliedRouteConfigurationIds();
        if (appliedIds != null && appliedIds.contains(requiredId)) {
            return true;
        }
        String configuredIds = definition.getRouteConfigurationId();
        if (configuredIds == null || configuredIds.isBlank()) {
            return false;
        }
        for (String configuredId : configuredIds.split(",")) {
            if (requiredId.equals(configuredId.trim())) {
                return true;
            }
        }
        return false;
    }

    private void validateBoundary(String routeId, MessageBoundaryKind boundaryKind) {
        if (boundaryKind == MessageBoundaryKind.KAFKA) {
            if (failureMode != MessageFailureMode.DLQ) {
                throw new IllegalArgumentException(
                        "Kafka boundary route '" + routeId + "' requires basis.message-store.failure-mode=dlq");
            }
            return;
        }

        if (boundaryKind != MessageBoundaryKind.HTTP) {
            throw new IllegalArgumentException(
                    "Non-Kafka boundary route '" + routeId
                            + "' uses a source that has no supported message-store profile");
        }

        if (replayEnabled) {
            throw new IllegalArgumentException(
                    "Non-Kafka boundary route '" + routeId
                            + "' requires basis.message-store.replay.enabled=false until a replay target is configured");
        }
        if (failureMode != MessageFailureMode.RECORD_FAILED) {
            throw new IllegalArgumentException(
                    "Non-Kafka boundary route '" + routeId
                            + "' requires basis.message-store.failure-mode=record-failed");
        }
    }
}
