package no.basis.integrationplatform.camel;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Validates guarded test profiles and exposes their effective runtime behavior. */
@ApplicationScoped
public class MessageStoreTestProfileConfig {
    /** Safe production default. */
    public static final String DEFAULT_PROFILE = "disabled";

    private static final Logger LOG = LoggerFactory.getLogger(MessageStoreTestProfileConfig.class);

    private final MessageStoreTestProfile profile;
    private final boolean testProfilesAllowed;

    /**
     * Creates and validates the effective test profile.
     *
     * @param profile configured profile
     * @param testProfilesAllowed explicit unsafe-profile opt-in
     */
    public MessageStoreTestProfileConfig(
            @ConfigProperty(name = "basis.message-store.test-profile",
                    defaultValue = DEFAULT_PROFILE) String profile,
            @ConfigProperty(name = "basis.message-store.allow-test-profiles",
                    defaultValue = "false") boolean testProfilesAllowed) {
        this.profile = MessageStoreTestProfile.fromConfig(profile);
        this.testProfilesAllowed = testProfilesAllowed;
        validate();
    }

    void onStartup(@Observes StartupEvent event) {
        if (!profile.active()) {
            return;
        }

        String noCommitGuidance = profile.kafkaCommitEnabled()
                ? ""
                : " A no-commit profile does not repeatedly deliver a record in the same running consumer;"
                        + " restart, seek, or reset the consumer offset before reading it again.";
        LOG.warn("MESSAGE-STORE TEST PROFILE ACTIVE: {}. persistenceEnabled={}, kafkaCommitEnabled={}. "
                        + "This mode is intended only for local or controlled integration testing.{}",
                profile.configValue(), profile.persistenceEnabled(), profile.kafkaCommitEnabled(), noCommitGuidance);
    }

    void validate() {
        if (profile.active() && !testProfilesAllowed) {
            throw new IllegalStateException(
                    "basis.message-store.test-profile=" + profile.configValue()
                            + " is test-only. Set basis.message-store.allow-test-profiles=true explicitly "
                            + "in a local or controlled test environment to enable it.");
        }
    }

    /**
     * Returns the parsed profile.
     *
     * @return parsed profile
     */
    public MessageStoreTestProfile profile() {
        return profile;
    }

    /**
     * Reports whether a non-default test profile is active.
     *
     * @return active state
     */
    public boolean active() {
        return profile.active();
    }

    /**
     * Reports whether configured durable persistence remains enabled.
     *
     * @return persistence state
     */
    public boolean persistenceEnabled() {
        return profile.persistenceEnabled();
    }

    /**
     * Reports whether adapter-managed Kafka commit remains enabled.
     *
     * @return commit state
     */
    public boolean kafkaCommitEnabled() {
        return profile.kafkaCommitEnabled();
    }

    /**
     * Resolves the sink after applying the profile.
     *
     * @param configuredSink configured sink
     * @return effective sink
     */
    public String effectiveSink(String configuredSink) {
        return persistenceEnabled() ? configuredSink : "log";
    }

    /**
     * Resolves the step store after applying the profile.
     *
     * @param configuredStore configured step store
     * @return effective step store
     */
    public String effectiveStepOutcomeStore(String configuredStore) {
        return persistenceEnabled() ? configuredStore : "noop";
    }
}
