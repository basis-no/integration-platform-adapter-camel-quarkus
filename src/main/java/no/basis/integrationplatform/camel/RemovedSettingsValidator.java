package no.basis.integrationplatform.camel;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.ConfigValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;

/**
 * Refuses to start with a setting of adapter 0.3.x that this version no longer reads, so that an
 * upgraded integration does not lose its effect without notice.
 */
@ApplicationScoped
class RemovedSettingsValidator {
    private static final Logger LOG = LoggerFactory.getLogger(RemovedSettingsValidator.class);

    private record Removed(String prefix, String property, String instead) {
        String envPrefix() {
            return prefix.replace('.', '_').replace('-', '_').toUpperCase(Locale.ROOT);
        }
    }

    private static final List<Removed> REMOVED = List.of(
            new Removed("basis.delivery-audit.", "basis.delivery-audit.routes",
                    "delivery audit is not available in adapter 0.4 yet (see "
                            + "https://github.com/basis-no/integration-platform-adapter-camel-quarkus/issues/93); "
                            + "remove the setting to start without it"),
            new Removed("basis.input-audit.", "basis.input-audit.routes",
                    "for an HTTP input, select the route as an audit-only HTTP boundary "
                            + "(basis.message-store.boundary-routes=<route id> with "
                            + "basis.message-store.failure-mode=record-failed and "
                            + "basis.message-store.replay.enabled=false), which cannot be combined with Kafka "
                            + "boundaries in the same application; for a file, timer or poller input, "
                            + "normalize it to Kafka (basis.message-store.ingress.normalization-routes); "
                            + "inspection of other non-Kafka inputs is not available in adapter 0.4 yet (see "
                            + "https://github.com/basis-no/integration-platform-adapter-camel-quarkus/issues/93)"));

    void validate(@Observes StartupEvent event) {
        check(ConfigProvider.getConfig());
    }

    static void check(Config config) {
        // The settings 0.3.x read, plus any other name in their namespaces. Each is resolved by its
        // dotted name, as 0.3.x resolved it: through every environment form and profile, and with
        // the value of the highest-ordinal source.
        Set<String> candidates = new TreeSet<>();
        for (Removed removed : REMOVED) {
            candidates.add(removed.property());
        }
        for (String name : config.getPropertyNames()) {
            String property = removedProperty(name);
            if (property != null) {
                candidates.add(property);
            }
        }
        Set<String> set = new TreeSet<>();
        Set<String> empty = new TreeSet<>();
        for (String property : candidates) {
            ConfigValue resolved = config.getConfigValue(property);
            String value = resolved.getValue();
            if (value != null && !value.isBlank()) {
                set.add(property);
            } else if (resolved.getSourceName() != null) {
                empty.add(property);
            }
        }
        for (String property : empty) {
            // An empty value disabled the feature in 0.3.x, so nothing is lost.
            LOG.warn("{} is a setting of adapter 0.3.x that this version does not read; it is empty, so nothing "
                    + "changes, but remove it", property);
        }
        if (set.isEmpty()) {
            return;
        }
        StringBuilder message = new StringBuilder(
                "Adapter 0.4 does not read these settings of adapter 0.3.x, so their effect would be lost:");
        for (String property : set) {
            message.append(' ').append(property).append(": ").append(insteadOf(property)).append('.');
        }
        throw new IllegalStateException(message.toString());
    }

    private static String removedProperty(String name) {
        if (name.startsWith("%")) {
            // Another profile's value; the active profile's value is also listed without its prefix.
            return null;
        }
        for (Removed removed : REMOVED) {
            if (name.startsWith(removed.prefix())) {
                return name;
            }
            if (name.startsWith(removed.envPrefix())) {
                return removed.prefix()
                        + name.substring(removed.envPrefix().length()).toLowerCase(Locale.ROOT).replace('_', '-');
            }
        }
        return null;
    }

    private static String insteadOf(String property) {
        for (Removed removed : REMOVED) {
            if (property.startsWith(removed.prefix())) {
                return removed.instead();
            }
        }
        throw new IllegalArgumentException(property);
    }
}
