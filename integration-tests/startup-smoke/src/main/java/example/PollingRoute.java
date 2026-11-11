package example;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.camel.builder.RouteBuilder;

import java.util.concurrent.atomic.AtomicInteger;

/** A recurring timer: a polling route the adapter would lease, here in an application without a Key-Value Store. */
@ApplicationScoped
public class PollingRoute extends RouteBuilder {
    static final AtomicInteger TICKS = new AtomicInteger();

    @Override
    public void configure() {
        from("timer:refresh?period=100").routeId("refresh").process(exchange -> TICKS.incrementAndGet());
    }
}
