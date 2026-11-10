package example;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.apache.camel.CamelContext;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Message Store datasource is inactive here, so the application has no Key-Value Store: its
 * client proxy fails on the first call. A polling route must then run uncoordinated, as before
 * leases, not stay stopped.
 */
@QuarkusTest
class PollingWithoutKeyValueStoreTest {
    @Inject CamelContext context;

    @Test
    void aPollingRouteRunsUncoordinatedWithoutAKeyValueStore() throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (PollingRoute.TICKS.get() < 3 && System.nanoTime() < deadline) {
            Thread.sleep(50);
        }
        assertTrue(PollingRoute.TICKS.get() >= 3, "the timer runs although the application has no Key-Value Store");
        assertTrue(context.getRoute("refresh").getRoutePolicyList().stream()
                        .anyMatch(policy -> policy.getClass().getSimpleName().equals("SingleActiveRoutePolicy")),
                "the route is a polling route the adapter would lease");
    }
}
