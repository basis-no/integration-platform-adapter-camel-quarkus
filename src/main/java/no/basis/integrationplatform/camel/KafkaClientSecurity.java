package no.basis.integrationplatform.camel;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;

/** Shared operator-owned authentication for adapter-created Kafka producers. */
public final class KafkaClientSecurity {
    private KafkaClientSecurity() {
    }

    /**
     * Applies the same security settings used by the Camel Kafka component.
     * Existing deployments default to plaintext; TLS always verifies hostnames.
     *
     * @param properties client properties to extend without logging credentials
     */
    public static void apply(Properties properties) {
        apply(properties, ConfigProvider.getConfig());
    }

    static void apply(Properties properties, Config config) {
        String protocol = config.getOptionalValue("basis.message-store.kafka.security-protocol", String.class)
                .orElse("PLAINTEXT");
        if (!Set.of("PLAINTEXT", "SSL", "SASL_SSL", "SASL_PLAINTEXT").contains(protocol)) {
            throw new IllegalArgumentException("Unsupported adapter Kafka security protocol");
        }
        properties.setProperty("security.protocol", protocol);
        if (protocol.startsWith("SASL")) {
            String mechanism = config.getOptionalValue("basis.message-store.kafka.sasl-mechanism", String.class)
                    .orElse("SCRAM-SHA-512");
            String jaas = config.getOptionalValue("basis.message-store.kafka.sasl-jaas-config", String.class)
                    .orElse("");
            if (!Set.of("SCRAM-SHA-512", "SCRAM-SHA-256", "PLAIN").contains(mechanism) || jaas.isBlank()) {
                throw new IllegalArgumentException("Adapter Kafka SASL requires a supported mechanism and credentials");
            }
            properties.setProperty("sasl.mechanism", mechanism);
            properties.setProperty("sasl.jaas.config", jaas);
        }
        if (protocol.endsWith("SSL")) {
            properties.setProperty("ssl.endpoint.identification.algorithm", "https");
            for (Map.Entry<String, String> mapping : Map.of(
                    "basis.message-store.kafka.ssl-truststore-location", "ssl.truststore.location",
                    "basis.message-store.kafka.ssl-truststore-type", "ssl.truststore.type").entrySet()) {
                config.getOptionalValue(mapping.getKey(), String.class)
                        .filter(value -> !value.isBlank())
                        .ifPresent(value -> properties.setProperty(mapping.getValue(), value));
            }
        }
    }
}
