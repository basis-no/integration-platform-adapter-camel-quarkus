package no.basis.integrationplatform.camel.step;

import io.quarkus.arc.DefaultBean;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import no.basis.integrationplatform.camel.MessageStoreTestProfileConfig;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.sql.DataSource;

/** Produces the configured durable or no-op step-outcome store. */
@ApplicationScoped
public class StepOutcomeStoreProducer {
    /** Creates the CDI producer. */
    public StepOutcomeStoreProducer() {
    }

    @Produces
    @DefaultBean
    @ApplicationScoped
    StepOutcomeStore stepOutcomeStore(
            @ConfigProperty(name = "basis.message-store.step-outcome-store",
                    defaultValue = StepOutcomeStoreModeValidator.NOOP) String mode,
            Instance<DataSource> dataSource,
            MessageStoreTestProfileConfig testProfile) {
        String effectiveMode = testProfile.effectiveStepOutcomeStore(mode);
        return switch (effectiveMode) {
            case StepOutcomeStoreModeValidator.POSTGRES -> new PostgresStepOutcomeStore(requiredDataSource(dataSource));
            case StepOutcomeStoreModeValidator.NOOP -> new NoopStepOutcomeStore();
            default -> throw new IllegalArgumentException(
                    "Unsupported basis.message-store.step-outcome-store value: " + effectiveMode
                            + ". Supported values: " + StepOutcomeStoreModeValidator.SUPPORTED_VALUES);
        };
    }

    private DataSource requiredDataSource(Instance<DataSource> dataSource) {
        if (dataSource.isResolvable()) {
            return dataSource.get();
        }
        throw new IllegalStateException("basis.message-store.step-outcome-store=postgres requires a JDBC DataSource");
    }
}
