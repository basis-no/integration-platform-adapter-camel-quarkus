package no.basis.integrationplatform.camel.ingress;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;

/** Exact route-id selection for asynchronous source-to-Kafka normalization. */
@ApplicationScoped
public class IngressNormalizationConfig {
    /** Adapter property containing comma-separated normalization route ids. */
    public static final String PROPERTY = "basis.message-store.ingress.normalization-routes";

    private final Set<String> routeIds;

    /**
     * Creates the CDI configuration from the optional adapter property.
     *
     * @param configuredRouteIds configured comma-separated route ids
     */
    @Inject
    public IngressNormalizationConfig(@ConfigProperty(name = PROPERTY) Optional<String> configuredRouteIds) {
        this(configuredRouteIds.orElse(""));
    }

    /**
     * Creates a configuration from comma-separated route ids.
     *
     * @param configuredRouteIds configured comma-separated route ids
     */
    public IngressNormalizationConfig(String configuredRouteIds) {
        this.routeIds = parseRouteIds(configuredRouteIds);
    }

    /**
     * Returns a configuration with no normalization routes.
     *
     * @return disabled configuration
     */
    public static IngressNormalizationConfig disabled() {
        return new IngressNormalizationConfig("");
    }

    /**
     * Tests whether a route is selected for normalization.
     *
     * @param routeId Camel route id
     * @return {@code true} when selected
     */
    public boolean includes(String routeId) {
        return routeId != null && routeIds.contains(routeId);
    }

    /**
     * Reports whether any route is selected.
     *
     * @return {@code true} when normalization is enabled
     */
    public boolean enabled() {
        return !routeIds.isEmpty();
    }

    /**
     * Returns the immutable selected route ids.
     *
     * @return selected route ids
     */
    public Set<String> routeIds() {
        return routeIds;
    }

    private Set<String> parseRouteIds(String configuredRouteIds) {
        if (configuredRouteIds == null || configuredRouteIds.isBlank()) {
            return Set.of();
        }
        Set<String> parsed = new LinkedHashSet<>();
        for (String value : configuredRouteIds.split(",")) {
            String routeId = value.trim();
            if (!routeId.isEmpty()) {
                parsed.add(routeId);
            }
        }
        return Set.copyOf(parsed);
    }
}
