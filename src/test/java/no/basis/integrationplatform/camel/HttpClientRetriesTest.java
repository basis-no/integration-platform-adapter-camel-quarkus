package no.basis.integrationplatform.camel;

import org.apache.camel.component.http.HttpComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Which HTTP components the adapter keeps from repeating requests by themselves. */
class HttpClientRetriesTest {
    /** An application's own subclass, as a framework or a proxy would make. */
    static class OwnHttpComponent extends HttpComponent {
    }

    @Test
    void turnsOffEveryHttpComponentInstanceUnderAnyName() throws Exception {
        try (var context = new DefaultCamelContext()) {
            var named = new OwnHttpComponent();
            context.addComponent("partner-http", named);
            HttpClientRetries.disableByDefault(context);
            var later = new HttpComponent();
            context.addComponent("later-http", later);

            assertTrue(named.isAutomaticRetriesDisabled(), "A subclass under its own name");
            assertTrue(later.isAutomaticRetriesDisabled(), "A component added after the configurations load");
            assertTrue(context.getComponent("http", HttpComponent.class).isAutomaticRetriesDisabled(),
                    "A component Camel creates on first use");
            assertTrue(context.getComponent("https", HttpComponent.class).isAutomaticRetriesDisabled());
        }
    }

    @Test
    void keepsTheApplicationsOwnValueForThatName() throws Exception {
        try (var context = new DefaultCamelContext()) {
            var properties = new Properties();
            properties.setProperty("camel.component.http.automatic-retries-disabled", "false");
            properties.setProperty("camel.component.partner-http.automaticRetriesDisabled", "false");
            context.getPropertiesComponent().setOverrideProperties(properties);
            var http = new HttpComponent();
            http.setAutomaticRetriesDisabled(true);
            context.addComponent("http", http);
            var partner = new OwnHttpComponent();
            context.addComponent("partner-http", partner);
            HttpClientRetries.disableByDefault(context);

            assertFalse(http.isAutomaticRetriesDisabled(),
                    "The application's value wins, also over an earlier binding");
            assertFalse(partner.isAutomaticRetriesDisabled());
            assertTrue(context.getComponent("https", HttpComponent.class).isAutomaticRetriesDisabled(),
                    "Another name keeps the adapter's default");
        }
    }
}
