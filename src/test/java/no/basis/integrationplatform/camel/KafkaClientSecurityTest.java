package no.basis.integrationplatform.camel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.smallrye.config.SmallRyeConfigBuilder;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class KafkaClientSecurityTest {
    @Test
    void plaintextCompatibilityAddsNoCredentials() {
        Properties properties = new Properties();
        KafkaClientSecurity.apply(properties, new SmallRyeConfigBuilder().build());
        assertEquals("PLAINTEXT", properties.getProperty("security.protocol"));
        assertFalse(properties.containsKey("sasl.jaas.config"));
    }

    @Test
    void authenticatedTlsPreservesVerificationAndCredentialsForEveryProducer() {
        Properties properties = new Properties();
        var config = new SmallRyeConfigBuilder()
                .withDefaultValue("basis.message-store.kafka.security-protocol", "SASL_SSL")
                .withDefaultValue("basis.message-store.kafka.sasl-jaas-config", "synthetic-credential")
                .withDefaultValue("basis.message-store.kafka.ssl-truststore-location", "/certificates/ca.crt")
                .withDefaultValue("basis.message-store.kafka.ssl-truststore-type", "PEM").build();
        KafkaClientSecurity.apply(properties, config);
        assertEquals("SCRAM-SHA-512", properties.getProperty("sasl.mechanism"));
        assertEquals("synthetic-credential", properties.getProperty("sasl.jaas.config"));
        assertEquals("https", properties.getProperty("ssl.endpoint.identification.algorithm"));
        assertEquals("PEM", properties.getProperty("ssl.truststore.type"));
        assertEquals("/certificates/ca.crt", properties.getProperty("ssl.truststore.location"));
    }

    @Test
    void missingCredentialsFailWithoutFallingBackToPlaintext() {
        var config = new SmallRyeConfigBuilder()
                .withDefaultValue("basis.message-store.kafka.security-protocol", "SASL_SSL").build();
        assertThrows(IllegalArgumentException.class, () -> KafkaClientSecurity.apply(new Properties(), config));
    }
}
