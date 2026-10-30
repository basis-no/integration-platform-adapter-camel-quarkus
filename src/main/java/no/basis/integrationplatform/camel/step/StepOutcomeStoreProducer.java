package no.basis.integrationplatform.camel.step;

import io.quarkus.arc.DefaultBean;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.sql.DataSource;

@ApplicationScoped
public class StepOutcomeStoreProducer {
    @Produces
    @DefaultBean
    @ApplicationScoped
    StepOutcomeStore stepOutcomeStore(
            @ConfigProperty(name = "basis.message-store.step-outcome-store",
                    defaultValue = StepOutcomeStoreModeValidator.NOOP) String mode,
            Instance<DataSource> dataSource) {
        return switch (mode) {
            case StepOutcomeStoreModeValidator.POSTGRES -> new PostgresStepOutcomeStore(requiredDataSource(dataSource));
            case StepOutcomeStoreModeValidator.NOOP -> new NoopStepOutcomeStore();
            default -> throw new IllegalArgumentException(
                    "Unsupported basis.message-store.step-outcome-store value: " + mode
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
