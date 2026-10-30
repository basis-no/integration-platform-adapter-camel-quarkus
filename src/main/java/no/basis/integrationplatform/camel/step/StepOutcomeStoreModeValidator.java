package no.basis.integrationplatform.camel.step;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import io.quarkus.runtime.StartupEvent;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class StepOutcomeStoreModeValidator {
    public static final String NOOP = "noop";
    public static final String POSTGRES = "postgres";
    public static final String SUPPORTED_VALUES = NOOP + ", " + POSTGRES;

    private final String mode;

    public StepOutcomeStoreModeValidator(
            @ConfigProperty(name = "basis.message-store.step-outcome-store", defaultValue = NOOP) String mode) {
        this.mode = mode;
    }

    void validate(@Observes StartupEvent event) {
        if (NOOP.equals(mode) || POSTGRES.equals(mode)) {
            return;
        }
        throw new IllegalArgumentException("Unsupported basis.message-store.step-outcome-store value: " + mode
                + ". Supported values: " + SUPPORTED_VALUES);
    }
}
