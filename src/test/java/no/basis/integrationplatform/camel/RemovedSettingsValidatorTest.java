package no.basis.integrationplatform.camel;

import io.smallrye.config.EnvConfigSource;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfigBuilder;
import org.eclipse.microprofile.config.Config;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RemovedSettingsValidatorTest {
    @Test
    void startsWithoutAnyRemovedSetting() {
        assertDoesNotThrow(() -> RemovedSettingsValidator.check(properties(Map.of(
                "basis.message-store.boundary-routes", "auto"))));
    }

    @Test
    void refusesDeliveryAuditAndNamesWhatIsMissing() {
        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> RemovedSettingsValidator.check(properties(Map.of(
                        "basis.delivery-audit.routes", "deliver-order"))));

        assertTrue(failure.getMessage().contains("basis.delivery-audit.routes: delivery audit is not available"));
        assertTrue(failure.getMessage().contains("/issues/93"));
        assertFalse(failure.getMessage().contains("deliver-order"), "names the setting, not its value");
    }

    @Test
    void refusesInputAuditAndNamesTheBoundariesThatReplaceIt() {
        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> RemovedSettingsValidator.check(properties(Map.of(
                        "basis.input-audit.routes", "receive-file"))));

        assertTrue(failure.getMessage().contains("basis.input-audit.routes: for an HTTP input"));
        assertTrue(failure.getMessage().contains("list its route id in basis.message-store.boundary-routes"));
        assertTrue(failure.getMessage().contains("basis.message-store.ingress.normalization-routes"));
    }

    @Test
    void refusesASettingFromTheEnvironment() {
        Config config = new SmallRyeConfigBuilder()
                .withSources(new EnvConfigSource(Map.of("BASIS_DELIVERY_AUDIT_ROUTES", "deliver-order"), 300))
                .build();

        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> RemovedSettingsValidator.check(config));

        assertTrue(failure.getMessage().contains("basis.delivery-audit.routes:"));
    }

    @Test
    void namesEachSettingOnceWhenItComesFromSeveralSources() {
        Config config = new SmallRyeConfigBuilder()
                .withSources(new EnvConfigSource(Map.of("BASIS_INPUT_AUDIT_ROUTES", "receive-file"), 300))
                .withSources(new PropertiesConfigSource(Map.of(
                        "basis.input-audit.routes", "receive-file",
                        "basis.delivery-audit.routes", "deliver-order"), "test", 250))
                .build();

        String message = assertThrows(IllegalStateException.class,
                () -> RemovedSettingsValidator.check(config)).getMessage();

        assertTrue(message.indexOf("basis.input-audit.routes:") == message.lastIndexOf("basis.input-audit.routes:"));
        assertTrue(message.contains("basis.delivery-audit.routes:"));
    }

    @Test
    void refusesTheActiveProfilesSettingButNotAnotherProfiles() {
        Config active = new SmallRyeConfigBuilder().withProfile("prod")
                .withSources(new PropertiesConfigSource(Map.of(
                        "%prod.basis.input-audit.routes", "receive-file"), "test", 250))
                .build();
        Config other = new SmallRyeConfigBuilder().withProfile("prod")
                .withSources(new PropertiesConfigSource(Map.of(
                        "%dev.basis.input-audit.routes", "receive-file"), "test", 250))
                .build();

        assertThrows(IllegalStateException.class, () -> RemovedSettingsValidator.check(active));
        assertDoesNotThrow(() -> RemovedSettingsValidator.check(other));
    }

    @Test
    void startsWithAnEmptyValueWhichDisabledTheFeatureIn03AndWarnsToRemoveIt() {
        try (var logs = LogCapture.of(RemovedSettingsValidator.class)) {
            assertDoesNotThrow(() -> RemovedSettingsValidator.check(properties(Map.of(
                    "basis.delivery-audit.routes", ""))));

            List<String> warnings = logs.at(Level.WARNING).stream().map(LogCapture::text).toList();
            assertEquals(1, warnings.size(), warnings.toString());
            assertTrue(warnings.get(0).startsWith("basis.delivery-audit.routes is a setting of adapter 0.3.x"));
        }
    }

    @Test
    void refusesAProfileSpecificEnvironmentVariable() {
        Config config = new SmallRyeConfigBuilder().withProfile("prod")
                .withSources(new EnvConfigSource(Map.of("_PROD_BASIS_DELIVERY_AUDIT_ROUTES", "deliver-order"), 300))
                .build();

        String message = assertThrows(IllegalStateException.class,
                () -> RemovedSettingsValidator.check(config)).getMessage();

        assertTrue(message.contains("basis.delivery-audit.routes:"));
    }

    @Test
    void refusesALowerCaseEnvironmentVariable() {
        Config config = new SmallRyeConfigBuilder()
                .withSources(new EnvConfigSource(Map.of("basis_input_audit_routes", "receive-file"), 300))
                .build();

        String message = assertThrows(IllegalStateException.class,
                () -> RemovedSettingsValidator.check(config)).getMessage();

        assertTrue(message.contains("basis.input-audit.routes:"));
    }

    @Test
    void refusesASystemProperty() {
        Config config = new SmallRyeConfigBuilder()
                .withSources(new PropertiesConfigSource(Map.of(
                        "basis.delivery-audit.routes", "deliver-order"), "SysPropConfigSource", 400))
                .build();

        assertThrows(IllegalStateException.class, () -> RemovedSettingsValidator.check(config));
    }

    @Test
    void followsTheEffectiveValueWhenAnEmptySystemPropertyOverridesTheEnvironment() {
        // 0.3.x saw the empty value, so the feature was off.
        Config config = new SmallRyeConfigBuilder()
                .withSources(new EnvConfigSource(Map.of("BASIS_DELIVERY_AUDIT_ROUTES", "deliver-order"), 300))
                .withSources(new PropertiesConfigSource(Map.of(
                        "basis.delivery-audit.routes", ""), "SysPropConfigSource", 400))
                .build();

        assertDoesNotThrow(() -> RemovedSettingsValidator.check(config));
    }

    private static Config properties(Map<String, String> values) {
        return new SmallRyeConfigBuilder()
                .withSources(new PropertiesConfigSource(values, "test", 250))
                .build();
    }
}
