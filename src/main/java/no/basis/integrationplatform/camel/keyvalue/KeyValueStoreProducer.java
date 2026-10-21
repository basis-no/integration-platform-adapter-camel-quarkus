package no.basis.integrationplatform.camel.keyvalue;

import io.quarkus.arc.DefaultBean;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.Optional;

/**
 * Produces the Key-Value Store on the platform Message Store datasource.
 *
 * <p>The bean is created on its first use, so an application that does not use
 * the Key-Value Store never touches {@code bip_key_value} and runs
 * unchanged against a Message Store before contract 1.2.</p>
 */
@ApplicationScoped
public class KeyValueStoreProducer {
    // Contract section 13 requires the sweep at least hourly, so the default is also the maximum.
    static final long MAX_SWEEP_INTERVAL_SECONDS = 3_600;

    /** Creates the CDI producer. */
    public KeyValueStoreProducer() {
    }

    @Produces
    @DefaultBean
    @ApplicationScoped
    KeyValueStore keyValueStore(
            @ConfigProperty(name = "app.name") Optional<String> integrationName,
            @ConfigProperty(name = "basis.message-store.key-value.sweep-interval-seconds",
                    defaultValue = "" + MAX_SWEEP_INTERVAL_SECONDS) long sweepIntervalSeconds,
            Instance<DataSource> dataSource) {
        // The message log falls back to a placeholder name; shared state under one would mix integrations.
        String integration = integrationName.filter(name -> !name.isBlank())
                .orElseThrow(() -> new IllegalStateException(
                        "Key-Value Store requires app.name, the integration identity its keys belong to"));
        if (sweepIntervalSeconds < 1 || sweepIntervalSeconds > MAX_SWEEP_INTERVAL_SECONDS) {
            throw new IllegalStateException("basis.message-store.key-value.sweep-interval-seconds must be 1 to "
                    + MAX_SWEEP_INTERVAL_SECONDS + ", was " + sweepIntervalSeconds);
        }
        if (!dataSource.isResolvable()) {
            throw new IllegalStateException("Key-Value Store requires a JDBC DataSource for the Message Store");
        }
        PostgresKeyValueStore state = new PostgresKeyValueStore(dataSource.get(), integration);
        state.startSweep(Duration.ofSeconds(sweepIntervalSeconds));
        return state;
    }

    void close(@Disposes KeyValueStore state) {
        if (state instanceof PostgresKeyValueStore postgres) {
            postgres.close();
        }
    }
}
