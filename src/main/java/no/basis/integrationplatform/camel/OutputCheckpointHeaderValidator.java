package no.basis.integrationplatform.camel;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * Refuses to start when the output checkpoint header list names a credential-like header,
 * by the rule that masks header values in parked DLQ records. A checkpoint is stored in the
 * Message Store for the payload retention and set on the exchange again on every replay, so
 * a credential in it would be kept and reused.
 */
@ApplicationScoped
class OutputCheckpointHeaderValidator {
    static final String PROPERTY = "basis.message-store.step.output-checkpoint.header-names";

    private final String headerNames;

    @Inject
    OutputCheckpointHeaderValidator(@ConfigProperty(name = PROPERTY) Optional<String> headerNames) {
        this(headerNames.orElse(""));
    }

    OutputCheckpointHeaderValidator(String headerNames) {
        this.headerNames = headerNames == null ? "" : headerNames;
    }

    void validateAtStartup(@Observes StartupEvent ignored) {
        validate();
    }

    void validate() {
        Set<String> refused = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (String value : headerNames.split(",")) {
            String name = value.trim();
            if (!name.isEmpty() && PlatformRouteConfiguration.isCredentialHeader(name)) {
                refused.add(name);
            }
        }
        if (!refused.isEmpty()) {
            throw new IllegalArgumentException(PROPERTY + " names credential-like headers "
                    + String.join(", ", refused) + ". A checkpoint is stored in the Message Store and "
                    + "restored on replay, so it must not hold credentials; remove them from the list and "
                    + "obtain a fresh credential on each attempt instead. A harmless header whose name "
                    + "matches the rule must be carried another way, for example as an exchange property "
                    + "in basis.message-store.step.output-checkpoint.property-names.");
        }
    }
}
