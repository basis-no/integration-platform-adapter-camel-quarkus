package no.basis.integrationplatform.camel;

import org.apache.camel.component.kafka.KafkaComponent;
import org.apache.camel.component.kafka.PollOnError;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.PropertyBindingSupport;
import org.apache.camel.util.StringHelper;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The Kafka consumer defaults the adapter ships to applications. */
class KafkaConsumerDefaultsTest {
    @Test
    void pollFailureReconnectsWithManualCommit() throws Exception {
        var defaults = new Properties();
        try (InputStream in = getClass().getResourceAsStream("/application.properties")) {
            defaults.load(in);
        }
        try (var context = new DefaultCamelContext()) {
            var kafka = new HashMap<String, Object>();
            defaults.stringPropertyNames().stream()
                    .filter(key -> key.startsWith("camel.component.kafka.") && !defaults.getProperty(key).contains("${"))
                    .forEach(key -> kafka.put(StringHelper.dashToCamelCase(key.substring("camel.component.kafka.".length())),
                            defaults.getProperty(key)));
            var component = context.getComponent("kafka", KafkaComponent.class);
            PropertyBindingSupport.build().withCamelContext(context).withTarget(component)
                    .withProperties(kafka).withMandatory(true).bind();

            // A poll failure reconnects instead of stopping the consumer or skipping an offset.
            assertEquals(PollOnError.RECONNECT, component.getConfiguration().getPollOnError());
            assertFalse(component.getConfiguration().isAutoCommitEnable());
            assertTrue(component.getConfiguration().isAllowManualCommit());
        }
    }
}
