package no.basis.integrationplatform.camel;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.time.Duration;

/** Refuses to start with Message Store timeouts that would not bound a call. */
@ApplicationScoped
class MessageStoreTimeoutsValidator {
    private final Duration statementTimeout;
    private final Duration lockTimeout;

    @Inject
    MessageStoreTimeoutsValidator(
            @ConfigProperty(name = "basis.message-store.statement-timeout", defaultValue = "10s") Duration statementTimeout,
            @ConfigProperty(name = "basis.message-store.lock-timeout", defaultValue = "5s") Duration lockTimeout) {
        this.statementTimeout = statementTimeout;
        this.lockTimeout = lockTimeout;
    }

    void validate(@Observes StartupEvent event) {
        MessageStoreTimeouts.of(statementTimeout, lockTimeout);
    }
}
