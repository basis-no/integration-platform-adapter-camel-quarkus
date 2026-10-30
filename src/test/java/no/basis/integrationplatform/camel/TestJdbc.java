package no.basis.integrationplatform.camel;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;

/**
 * Waits for a Testcontainers-mapped JDBC port to become reachable from the host.
 *
 * <p>Some container runtimes (e.g. Rancher Desktop) program host port forwards
 * asynchronously after container start, so the first host connection can be
 * refused even though the database already logged readiness inside the
 * container.</p>
 */
public final class TestJdbc {
    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    private static final long RETRY_DELAY_MILLIS = 250;

    private TestJdbc() {
    }

    public static void awaitReachable(DataSource dataSource) {
        Instant deadline = Instant.now().plus(TIMEOUT);
        Exception lastFailure = null;
        while (Instant.now().isBefore(deadline)) {
            try (Connection connection = dataSource.getConnection()) {
                if (connection.isValid(5)) {
                    return;
                }
            } catch (Exception e) {
                lastFailure = e;
            }
            try {
                Thread.sleep(RETRY_DELAY_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("Interrupted while waiting for the test database", e);
            }
        }
        throw new AssertionError("Test database was not reachable within " + TIMEOUT, lastFailure);
    }
}
