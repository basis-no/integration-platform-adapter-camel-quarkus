package example;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.camel.builder.RouteBuilder;

import java.util.concurrent.atomic.AtomicInteger;

/** Two polling routes: one the adapter runs on one replica through its lease, one excluded from it. */
@ApplicationScoped
public class PollingRoutes extends RouteBuilder {
    static final AtomicInteger LEASED_TICKS = new AtomicInteger();
    static final AtomicInteger EXCLUDED_TICKS = new AtomicInteger();

    @Override
    public void configure() {
        from("timer:leased?period=200").routeId("leased-tick").process(exchange -> LEASED_TICKS.incrementAndGet());
        from("timer:excluded?period=200").routeId("excluded-tick").process(exchange -> EXCLUDED_TICKS.incrementAndGet());
    }
}
