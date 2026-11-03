package no.basis.integrationplatform.camel.step;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import io.quarkus.runtime.StartupEvent;
import jakarta.inject.Inject;
import no.basis.integrationplatform.camel.MessageStoreDataSource;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.sql.DataSource;

@ApplicationScoped
public class StepOutcomeStoreModeValidator {
    public static final String NOOP = "noop";
    public static final String POSTGRES = "postgres";
    public static final String SUPPORTED_VALUES = NOOP + ", " + POSTGRES;

    private final String mode;
    private final Instance<DataSource> dataSource;

    public StepOutcomeStoreModeValidator(String mode) {
        this(mode, null);
    }

    @Inject
    public StepOutcomeStoreModeValidator(
            @ConfigProperty(name = "basis.message-store.step-outcome-store", defaultValue = NOOP) String mode,
            Instance<DataSource> dataSource) {
        this.mode = mode;
        this.dataSource = dataSource;
    }

    void validate(@Observes StartupEvent event) {
        if (POSTGRES.equals(mode) && dataSource != null && dataSource.isAmbiguous()) {
            // Otherwise the app reports ready and every tracked send fails.
            throw MessageStoreDataSource.ambiguous("basis.message-store.step-outcome-store=postgres");
        }
        if (NOOP.equals(mode) || POSTGRES.equals(mode)) {
            return;
        }
        throw new IllegalArgumentException("Unsupported basis.message-store.step-outcome-store value: " + mode
                + ". Supported values: " + SUPPORTED_VALUES);
    }
}
