package no.basis.integrationplatform.camel.step;

import io.quarkus.arc.DefaultBean;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Default replay-semantic classifier, replaceable with an application CDI bean. */
@ApplicationScoped
@DefaultBean
public class DefaultStepClassifier implements StepClassifier {
    private static final Set<String> INTERNAL_SCHEMES = Set.of(
            "bean", "controlbus", "dataformat", "direct", "language", "log", "mock",
            "micrometer", "seda", "stub", "timer", "validator", "vm", "xslt", "xslt-saxon");

    private static final Set<String> WRITE_ONLY_PRODUCER_SCHEMES = Set.of(
            "amqp", "amqp10", "file", "ftp", "ftps", "jms", "kafka",
            "sftp", "sjms", "sjms2");

    private static final Set<String> HTTP_SCHEMES = Set.of(
            "ahc", "http", "https", "netty-http", "platform-http", "vertx-http");

    private static final Set<String> SAFE_HTTP_METHODS = Set.of("GET", "HEAD", "OPTIONS", "TRACE");

    private final Map<String, StepRole> nodeRoles;

    /**
     * Builds a classifier from exact Camel node-id lists.
     *
     * @param internalIds comma-separated internal node ids
     * @param repeatableIds comma-separated repeatable node ids
     * @param sideEffectIds comma-separated durable side-effect node ids
     * @param outputSideEffectIds comma-separated output-producing side-effect node ids
     */
    @Inject
    public DefaultStepClassifier(
            @ConfigProperty(name = "basis.message-store.step.internal-ids") Optional<String> internalIds,
            @ConfigProperty(name = "basis.message-store.step.repeatable-ids") Optional<String> repeatableIds,
            @ConfigProperty(name = "basis.message-store.step.side-effect-ids") Optional<String> sideEffectIds,
            @ConfigProperty(name = "basis.message-store.step.output-side-effect-ids") Optional<String> outputSideEffectIds) {
        this(internalIds.orElse(""), repeatableIds.orElse(""), sideEffectIds.orElse(""), outputSideEffectIds.orElse(""));
    }

    DefaultStepClassifier(String internalIds, String repeatableIds, String sideEffectIds) {
        this(internalIds, repeatableIds, sideEffectIds, "");
    }

    DefaultStepClassifier(String internalIds, String repeatableIds, String sideEffectIds, String outputSideEffectIds) {
        this.nodeRoles = configuredNodeRoles(internalIds, repeatableIds, sideEffectIds, outputSideEffectIds);
    }

    /**
     * Returns a classifier with automatic scheme and HTTP-method rules only.
     *
     * @return default classifier without explicit node-id overrides
     */
    public static DefaultStepClassifier defaults() {
        return new DefaultStepClassifier("", "", "", "");
    }

    void validateAtStartup(@Observes StartupEvent ignored) {
        // Construction validates conflicting node-role configuration.
    }

    @Override
    public StepRole classify(StepClassificationContext context) {
        StepRole configuredRole = context.nodeId() == null ? null : nodeRoles.get(context.nodeId());
        if (configuredRole != null) {
            return configuredRole;
        }

        String scheme = context.scheme();
        if (INTERNAL_SCHEMES.contains(scheme)) {
            return StepRole.INTERNAL;
        }
        if (WRITE_ONLY_PRODUCER_SCHEMES.contains(scheme)) {
            return StepRole.SIDE_EFFECT;
        }
        String httpMethod = context.httpMethod();
        if (HTTP_SCHEMES.contains(scheme) && httpMethod != null && SAFE_HTTP_METHODS.contains(httpMethod)) {
            return StepRole.REPEATABLE;
        }
        return StepRole.AMBIGUOUS;
    }

    private Map<String, StepRole> configuredNodeRoles(
            String internalIds,
            String repeatableIds,
            String sideEffectIds,
            String outputSideEffectIds) {
        Map<String, StepRole> roles = new HashMap<>();
        addRoles(roles, parseIds(internalIds), StepRole.INTERNAL);
        addRoles(roles, parseIds(repeatableIds), StepRole.REPEATABLE);
        addRoles(roles, parseIds(sideEffectIds), StepRole.SIDE_EFFECT);
        addRoles(roles, parseIds(outputSideEffectIds), StepRole.OUTPUT_SIDE_EFFECT);
        return Map.copyOf(roles);
    }

    private void addRoles(Map<String, StepRole> roles, Set<String> nodeIds, StepRole role) {
        for (String nodeId : nodeIds) {
            StepRole existing = roles.putIfAbsent(nodeId, role);
            if (existing != null && existing != role) {
                throw new IllegalArgumentException(
                        "Step node id '" + nodeId + "' is configured as both "
                                + existing.name().toLowerCase(Locale.ROOT) + " and "
                                + role.name().toLowerCase(Locale.ROOT));
            }
        }
    }

    private Set<String> parseIds(String configuredIds) {
        if (configuredIds == null || configuredIds.isBlank()) {
            return Set.of();
        }
        Set<String> ids = new LinkedHashSet<>();
        for (String value : configuredIds.split(",")) {
            String id = value.trim();
            if (!id.isEmpty()) {
                ids.add(id);
            }
        }
        return Set.copyOf(ids);
    }
}
