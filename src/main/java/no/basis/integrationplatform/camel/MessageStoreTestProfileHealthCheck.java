package no.basis.integrationplatform.camel;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Readiness;

/** Readiness contribution exposing unsafe test-profile state to operators. */
@Readiness
@ApplicationScoped
public class MessageStoreTestProfileHealthCheck implements HealthCheck {
    private final MessageStoreTestProfileConfig config;

    /**
     * Creates the readiness check.
     *
     * @param config effective profile configuration
     */
    public MessageStoreTestProfileHealthCheck(MessageStoreTestProfileConfig config) {
        this.config = config;
    }

    @Override
    public HealthCheckResponse call() {
        return HealthCheckResponse.named("basis-message-store-test-profile")
                .up()
                .withData("profile", config.profile().configValue())
                .withData("unsafeTestProfile", config.active())
                .withData("persistenceEnabled", config.persistenceEnabled())
                .withData("kafkaCommitEnabled", config.kafkaCommitEnabled())
                .build();
    }
}
