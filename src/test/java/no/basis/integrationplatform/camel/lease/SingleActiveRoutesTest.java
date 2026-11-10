package no.basis.integrationplatform.camel.lease;

import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.model.RouteDefinition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SingleActiveRoutesTest {
    private final DefaultCamelContext context = new DefaultCamelContext();

    @AfterEach
    void stop() {
        context.stop();
    }

    @Test
    void coordinatesScheduledSourcesBySchemeAndPollingEndpointsByType() {
        assertTrue(coordinates("timer:poll?period=60000"));
        assertTrue(coordinates("quartz:nightly?cron=0+0+2+*+*+?"));
        assertTrue(coordinates("cron:nightly?schedule=0+0+2+*+*+?"));
        assertTrue(coordinates("scheduler:poll?delay=1000"));
        assertTrue(coordinates("file:/input?readLock=changed&move=.done"), "a directory consumer polls");
    }

    @Test
    void leavesSharedAndSelfCoordinatingSourcesAlone() {
        assertFalse(coordinates("direct:transform"));
        assertFalse(coordinates("kafka:orders?brokers=localhost:1"));
        assertFalse(coordinates("master:poll-orders:file:/input"), "a master: route coordinates itself");
        assertFalse(SingleActiveRoutes.coordinates(context, new RouteDefinition()), "no input, nothing to poll");
    }

    @Test
    void resolvesPlaceholdersBeforeDeciding() {
        context.getPropertiesComponent().addInitialProperty("orders.source", "timer:poll?period=60000");

        assertTrue(coordinates("{{orders.source}}"));
    }

    private boolean coordinates(String uri) {
        return SingleActiveRoutes.coordinates(context, new RouteDefinition().from(uri));
    }
}
