package no.basis.integrationplatform.camel.step;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import io.quarkus.runtime.StartupEvent;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import no.basis.integrationplatform.camel.ingress.IngressNormalizationConfig;

/** Validates the configured durable step-outcome store at application startup. */
@ApplicationScoped
public class StepOutcomeStoreModeValidator {
    /** Disables durable step-outcome tracking. */
    public static final String NOOP = "noop";
    /** Stores durable step outcomes in the configured Postgres data source. */
    public static final String POSTGRES = "postgres";
    /** Comma-separated supported store modes. */
    public static final String SUPPORTED_VALUES = NOOP + ", " + POSTGRES;

    private final String mode;
    private final IngressNormalizationConfig ingressNormalizationConfig;

    /**
     * Creates the startup validator.
     *
     * @param mode configured store mode
     * @param ingressNormalizationConfig ingress normalization configuration
     */
    public StepOutcomeStoreModeValidator(
            @ConfigProperty(name = "basis.message-store.step-outcome-store", defaultValue = NOOP) String mode,
            IngressNormalizationConfig ingressNormalizationConfig) {
        this.mode = mode;
        this.ingressNormalizationConfig = ingressNormalizationConfig;
    }

    void validate(@Observes StartupEvent event) {
        if (!NOOP.equals(mode) && !POSTGRES.equals(mode)) {
            throw new IllegalArgumentException("Unsupported basis.message-store.step-outcome-store value: " + mode
                    + ". Supported values: " + SUPPORTED_VALUES);
        }
        if (ingressNormalizationConfig.enabled() && !POSTGRES.equals(mode)) {
            throw new IllegalArgumentException(IngressNormalizationConfig.PROPERTY
                    + " requires basis.message-store.step-outcome-store=postgres so completed Kafka publishes "
                    + "can be recognized when the source redelivers an item");
        }
    }
}
