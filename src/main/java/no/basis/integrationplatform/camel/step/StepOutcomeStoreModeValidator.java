package no.basis.integrationplatform.camel.step;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import io.quarkus.runtime.StartupEvent;
import jakarta.inject.Inject;
import no.basis.integrationplatform.camel.MessageStoreDataSource;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import no.basis.integrationplatform.camel.ingress.IngressNormalizationConfig;

import javax.sql.DataSource;

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
    private final Instance<DataSource> dataSource;

    /**
     * Creates the startup validator without a DataSource check.
     *
     * @param mode configured store mode
     * @param ingressNormalizationConfig ingress normalization configuration
     */
    public StepOutcomeStoreModeValidator(String mode, IngressNormalizationConfig ingressNormalizationConfig) {
        this(mode, ingressNormalizationConfig, null);
    }

    StepOutcomeStoreModeValidator(String mode, Instance<DataSource> dataSource) {
        this(mode, IngressNormalizationConfig.disabled(), dataSource);
    }

    /**
     * Creates the startup validator.
     *
     * @param mode configured store mode
     * @param ingressNormalizationConfig ingress normalization configuration
     * @param dataSource the application's DataSource beans, checked for ambiguity with the Postgres store
     */
    @Inject
    public StepOutcomeStoreModeValidator(
            @ConfigProperty(name = "basis.message-store.step-outcome-store", defaultValue = NOOP) String mode,
            IngressNormalizationConfig ingressNormalizationConfig,
            Instance<DataSource> dataSource) {
        this.mode = mode;
        this.ingressNormalizationConfig = ingressNormalizationConfig;
        this.dataSource = dataSource;
    }

    void validate(@Observes StartupEvent event) {
        if (!NOOP.equals(mode) && !POSTGRES.equals(mode)) {
            throw new IllegalArgumentException("Unsupported basis.message-store.step-outcome-store value: " + mode
                    + ". Supported values: " + SUPPORTED_VALUES);
        }
        if (POSTGRES.equals(mode) && dataSource != null && dataSource.isAmbiguous()) {
            // Otherwise the app reports ready and every tracked send fails.
            throw MessageStoreDataSource.ambiguous("basis.message-store.step-outcome-store=postgres");
        }
        if (ingressNormalizationConfig.enabled() && !POSTGRES.equals(mode)) {
            throw new IllegalArgumentException(IngressNormalizationConfig.PROPERTY
                    + " requires basis.message-store.step-outcome-store=postgres so completed Kafka publishes "
                    + "can be recognized when the source redelivers an item");
        }
    }
}
