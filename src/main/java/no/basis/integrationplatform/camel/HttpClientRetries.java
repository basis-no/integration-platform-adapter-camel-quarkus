package no.basis.integrationplatform.camel;

import org.apache.camel.CamelContext;
import org.apache.camel.Component;
import org.apache.camel.support.LifecycleStrategySupport;
import org.apache.camel.support.PropertyBindingSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * Turns off the retries that the HTTP client inside Camel's {@code http} component makes by itself,
 * so that every repeated request is a Camel redelivery the adapter records and times.
 *
 * <p>It is set on each component instance, not in the adapter's packaged
 * {@code application.properties}: Camel refuses to start on a {@code camel.component.http.*}
 * property when the application has no {@code http} component. It covers every instance of Camel's
 * {@code HttpComponent}, a subclass included, under any name ({@code http}, {@code https} or an
 * application's own), also one added after the route configurations load. An application that
 * sets {@code camel.component.<name>.automatic-retries-disabled} for that name keeps its own value:
 * the adapter applies the resolved value itself, so the outcome does not depend on the order in
 * which Camel configures the component.
 */
final class HttpClientRetries {
    private static final Logger LOG = LoggerFactory.getLogger(HttpClientRetries.class);
    private static final String COMPONENT_CLASS = "org.apache.camel.component.http.HttpComponent";

    private HttpClientRetries() {
    }

    static void disableByDefault(CamelContext context) {
        Class<?> type = componentClass(context);
        if (type == null) {
            return;
        }
        for (String name : List.copyOf(context.getComponentNames())) {
            apply(context, type, name, context.hasComponent(name));
        }
        context.addLifecycleStrategy(new LifecycleStrategySupport() {
            @Override
            public void onComponentAdd(String name, Component component) {
                apply(context, type, name, component);
            }
        });
    }

    private static void apply(CamelContext context, Class<?> type, String name, Component component) {
        if (component == null || !type.isInstance(component)) {
            return;
        }
        String value = configuredValue(context, name).orElse("true");
        try {
            PropertyBindingSupport.build().withCamelContext(context).withTarget(component)
                    .withProperty("automaticRetriesDisabled", value).withMandatory(true).bind();
        } catch (RuntimeException unsupported) {
            LOG.warn("Could not set the {} component's own client retries; a 503 or 429 may be "
                    + "repeated outside the adapter's redeliveries: {}", name, unsupported.getMessage());
        }
    }

    private static Optional<String> configuredValue(CamelContext context, String name) {
        for (String option : List.of("automatic-retries-disabled", "automaticRetriesDisabled")) {
            Optional<String> value = context.getPropertiesComponent()
                    .resolveProperty("camel.component." + name + "." + option);
            if (value.isPresent()) {
                return Optional.of(value.get().trim().toLowerCase(Locale.ROOT));
            }
        }
        return Optional.empty();
    }

    private static Class<?> componentClass(CamelContext context) {
        try {
            return context.getClassResolver().resolveMandatoryClass(COMPONENT_CLASS);
        } catch (ClassNotFoundException absent) {
            return null;
        }
    }
}
