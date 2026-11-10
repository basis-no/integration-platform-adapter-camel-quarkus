package example;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.apache.camel.CamelContext;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The adapter's CDI wiring leases every polling route automatically and leaves an excluded one alone. */
@QuarkusTest
@QuarkusTestResource(KeyValuePostgresResource.class)
class SingleActiveRuntimeTest {
    @Inject DataSource dataSource;
    @Inject CamelContext camelContext;

    @Test
    void aPollingRouteRunsUnderItsLeaseAndAnExcludedOneWithout() throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while ((PollingRoutes.LEASED_TICKS.get() == 0 || PollingRoutes.EXCLUDED_TICKS.get() == 0)
                && System.nanoTime() < deadline) {
            Thread.sleep(50);
        }
        assertTrue(PollingRoutes.LEASED_TICKS.get() > 0, "the leased route runs on the replica that holds its lease");
        assertTrue(PollingRoutes.EXCLUDED_TICKS.get() > 0, "the excluded route runs");
        assertTrue(camelContext.getRoute("leased-tick").getRoutePolicyList().stream()
                .anyMatch(policy -> policy.getClass().getSimpleName().equals("SingleActiveRoutePolicy")),
                "the adapter registered its policy on the polling route by itself");
        assertFalse(camelContext.getRoute("excluded-tick").getRoutePolicyList().stream()
                .anyMatch(policy -> policy.getClass().getSimpleName().equals("SingleActiveRoutePolicy")));
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            try (var result = statement.executeQuery("select value->>'holder' from bip_key_value"
                    + " where integration = 'key-value-smoke' and key = 'lease/leased-tick'")) {
                assertTrue(result.next(), "the lease is stored in the Key-Value Store under the integration's identity");
                assertFalse(result.getString(1).isBlank());
            }
            try (var result = statement.executeQuery("select count(*) from bip_key_value"
                    + " where integration = 'key-value-smoke' and key = 'lease/excluded-tick'")) {
                assertTrue(result.next());
                assertEquals(0, result.getInt(1), "an excluded route takes no lease");
            }
        }
    }
}
