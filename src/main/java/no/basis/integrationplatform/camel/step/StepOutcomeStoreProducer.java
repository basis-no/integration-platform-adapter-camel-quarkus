package no.basis.integrationplatform.camel.step;

import io.quarkus.arc.DefaultBean;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import no.basis.integrationplatform.camel.MessageStoreDataSource;
import no.basis.integrationplatform.camel.MessageStoreTestProfileConfig;
import no.basis.integrationplatform.camel.MessageStoreTimeouts;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.sql.DataSource;
import java.time.Duration;

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
            @ConfigProperty(name = "basis.message-store.statement-timeout", defaultValue = "10s") Duration statementTimeout,
            @ConfigProperty(name = "basis.message-store.lock-timeout", defaultValue = "5s") Duration lockTimeout,
            Instance<DataSource> dataSource,
            MessageStoreTestProfileConfig testProfile) {
        String effectiveMode = testProfile.effectiveStepOutcomeStore(mode);
        return switch (effectiveMode) {
            case StepOutcomeStoreModeValidator.POSTGRES -> new PostgresStepOutcomeStore(requiredDataSource(dataSource),
                    MessageStoreTimeouts.of(statementTimeout, lockTimeout));
            case StepOutcomeStoreModeValidator.NOOP -> new NoopStepOutcomeStore();
            default -> throw new IllegalArgumentException(
                    "Unsupported basis.message-store.step-outcome-store value: " + effectiveMode
                            + ". Supported values: " + StepOutcomeStoreModeValidator.SUPPORTED_VALUES);
        };
    }

    private DataSource requiredDataSource(Instance<DataSource> dataSource) {
        return MessageStoreDataSource.require(dataSource, "basis.message-store.step-outcome-store=postgres");
    }
}
