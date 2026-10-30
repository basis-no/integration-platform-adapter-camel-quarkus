package no.basis.integrationplatform.camel;

import org.eclipse.microprofile.health.HealthCheckResponse;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageStoreTestProfileConfigTest {
    @Test
    void keepsProductionBehaviorWhenProfileIsDisabled() {
        MessageStoreTestProfileConfig config = new MessageStoreTestProfileConfig("disabled", false);

        config.validate();

        assertFalse(config.active());
        assertTrue(config.persistenceEnabled());
        assertTrue(config.kafkaCommitEnabled());
        assertEquals("postgres", config.effectiveSink("postgres"));
        assertEquals("postgres", config.effectiveStepOutcomeStore("postgres"));
    }

    @Test
    void noPersistenceProfileForcesNonDurableStoresButStillCommits() {
        MessageStoreTestProfileConfig config = new MessageStoreTestProfileConfig("no-persistence", true);

        config.validate();

        assertEquals("log", config.effectiveSink("postgres"));
        assertEquals("noop", config.effectiveStepOutcomeStore("postgres"));
        assertTrue(config.kafkaCommitEnabled());
    }

    @Test
    void combinedProfileDisablesPersistenceAndKafkaCommit() {
        MessageStoreTestProfileConfig config = new MessageStoreTestProfileConfig(
                "no-persistence-no-commit",
                true);

        config.validate();

        assertFalse(config.persistenceEnabled());
        assertFalse(config.kafkaCommitEnabled());
    }

    @Test
    void refusesTestProfileWithoutExplicitOptIn() {
        IllegalStateException thrown = assertThrows(
                IllegalStateException.class,
                () -> new MessageStoreTestProfileConfig("no-commit", false));

        assertTrue(thrown.getMessage().contains("basis.message-store.allow-test-profiles=true"));
    }

    @Test
    void rejectsUnknownProfile() {
        IllegalArgumentException thrown = assertThrows(
                IllegalArgumentException.class,
                () -> new MessageStoreTestProfileConfig("surprise", true));

        assertTrue(thrown.getMessage().contains(MessageStoreTestProfile.SUPPORTED_VALUES));
    }

    @Test
    void exposesUnsafeModeThroughReadinessData() {
        MessageStoreTestProfileConfig config = new MessageStoreTestProfileConfig("no-commit", true);
        HealthCheckResponse response = new MessageStoreTestProfileHealthCheck(config).call();
        Map<String, Object> data = response.getData().orElseThrow();

        assertEquals(HealthCheckResponse.Status.UP, response.getStatus());
        assertEquals("no-commit", data.get("profile"));
        assertEquals(true, data.get("unsafeTestProfile"));
        assertEquals(true, data.get("persistenceEnabled"));
        assertEquals(false, data.get("kafkaCommitEnabled"));
    }
}
