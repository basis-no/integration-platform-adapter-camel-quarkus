package no.basis.integrationplatform.camel.keyvalue;

import io.quarkus.arc.DefaultBean;
import io.quarkus.arc.InactiveBeanException;
import io.quarkus.arc.InjectableInstance;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import no.basis.integrationplatform.camel.MessageStoreDataSource;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.Optional;

/**
 * Produces the Key-Value Store on the Message Store datasource.
 *
 * <p>The bean is created on its first use, so an application that does not use
 * the Key-Value Store never touches {@code bip_key_value} and runs
 * unchanged against a Message Store without that table.</p>
 *
 * <p>Without an {@code app.name}, without a Message Store datasource, or with an
 * inactive one ({@code quarkus.datasource.active=false}), the store cannot be
 * created and its first call fails with {@link KeyValueStoreNotConfiguredException}.</p>
 */
@ApplicationScoped
public class KeyValueStoreProducer {
    // Expired keys must be swept at least hourly, so the default is also the maximum.
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
                .orElseThrow(() -> new KeyValueStoreNotConfiguredException(
                        "Key-Value Store requires app.name, the integration identity its keys belong to"));
        if (sweepIntervalSeconds < 1 || sweepIntervalSeconds > MAX_SWEEP_INTERVAL_SECONDS) {
            throw new IllegalStateException("basis.message-store.key-value.sweep-interval-seconds must be 1 to "
                    + MAX_SWEEP_INTERVAL_SECONDS + ", was " + sweepIntervalSeconds);
        }
        if (dataSource.isAmbiguous()) {
            throw MessageStoreDataSource.ambiguous("Key-Value Store");
        }
        if (!dataSource.isResolvable()) {
            throw new KeyValueStoreNotConfiguredException("Key-Value Store requires a JDBC DataSource for the Message Store");
        }
        // The datasource is a client proxy: a switched-off one (quarkus.datasource.active=false) resolves
        // here and fails only on its first connection, so ask whether its bean is active first.
        if (dataSource instanceof InjectableInstance<DataSource> injectable
                && !injectable.getHandle().getBean().isActive()) {
            throw new KeyValueStoreNotConfiguredException("Key-Value Store requires an active JDBC DataSource for"
                    + " the Message Store: " + injectable.getHandle().getBean().checkActive().inactiveReason());
        }
        DataSource resolved;
        try {
            resolved = dataSource.get();
        } catch (InactiveBeanException e) {
            throw new KeyValueStoreNotConfiguredException(
                    "Key-Value Store requires an active JDBC DataSource for the Message Store", e);
        }
        PostgresKeyValueStore state = new PostgresKeyValueStore(resolved, integration);
        state.startSweep(Duration.ofSeconds(sweepIntervalSeconds));
        return state;
    }

    void close(@Disposes KeyValueStore state) {
        if (state instanceof PostgresKeyValueStore postgres) {
            postgres.close();
        }
    }
}
