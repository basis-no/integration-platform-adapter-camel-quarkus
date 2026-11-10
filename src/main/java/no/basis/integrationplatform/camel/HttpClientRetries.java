package no.basis.integrationplatform.camel;

import org.apache.camel.CamelContext;
import org.apache.camel.Component;
import org.apache.camel.support.PropertyBindingSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * Turns off the retries that the HTTP client inside Camel's {@code http} and {@code https}
 * components makes by itself, so that every repeated request is a Camel redelivery the adapter
 * records and times.
 *
 * <p>It is set on the component, not in the adapter's packaged {@code application.properties}:
 * Camel refuses to start on a {@code camel.component.http.*} property when the application has no
 * {@code http} component. An application that sets
 * {@code camel.component.http.automatic-retries-disabled} (or the {@code https} one) itself keeps
 * its own value.
 */
final class HttpClientRetries {
    private static final Logger LOG = LoggerFactory.getLogger(HttpClientRetries.class);
    private static final List<String> SCHEMES = List.of("http", "https");
    private static final String COMPONENT_CLASS = "org.apache.camel.component.http.HttpComponent";

    private HttpClientRetries() {
    }

    static void disableByDefault(CamelContext context) {
        if (!onClasspath(context)) {
            return;
        }
        for (String scheme : SCHEMES) {
            if (configuredByApplication(context, scheme)) {
                continue;
            }
            Component component = context.getComponent(scheme);
            if (component == null || !COMPONENT_CLASS.equals(component.getClass().getName())) {
                continue;
            }
            try {
                PropertyBindingSupport.build().withCamelContext(context).withTarget(component)
                        .withProperty("automaticRetriesDisabled", "true").withMandatory(true).bind();
            } catch (RuntimeException unsupported) {
                LOG.warn("Could not turn off the {} component's own client retries; a 503 or 429 may be "
                        + "repeated outside the adapter's redeliveries: {}", scheme, unsupported.getMessage());
            }
        }
    }

    private static boolean onClasspath(CamelContext context) {
        try {
            context.getClassResolver().resolveMandatoryClass(COMPONENT_CLASS);
            return true;
        } catch (ClassNotFoundException absent) {
            return false;
        }
    }

    private static boolean configuredByApplication(CamelContext context, String scheme) {
        for (String option : List.of("automatic-retries-disabled", "automaticRetriesDisabled")) {
            Optional<String> value = context.getPropertiesComponent()
                    .resolveProperty("camel.component." + scheme + "." + option);
            if (value.isPresent()) {
                return true;
            }
        }
        return false;
    }
}
