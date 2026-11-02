package no.basis.integrationplatform.camel.step;

import no.basis.integrationplatform.camel.PlatformExchangeProperties;
import org.apache.camel.Exchange;

import java.util.concurrent.atomic.AtomicReference;

/**
 * The first failure to check or record a step outcome in a message's attempt, for example
 * during a Message Store outage. While it is set, the input is neither parked nor committed,
 * so it is consumed again. Split parts, loop iterations and branches run on copies of the
 * exchange, so the input route starts one holder that the copies share.
 */
public final class StepOutcomeFailure {
    private StepOutcomeFailure() {
    }

    /**
     * Starts the attempt's holder, which copies of the exchange share.
     * @param exchange the input exchange, when its route begins
     */
    public static void begin(Exchange exchange) {
        exchange.setProperty(PlatformExchangeProperties.STEP_OUTCOME_FAILURE, new AtomicReference<Throwable>());
    }

    /**
     * The attempt's first failure to check or record a step outcome.
     * @param exchange the input exchange or a copy of it
     * @return the failure, or {@code null} when every step outcome was checked and recorded
     */
    public static Throwable of(Exchange exchange) {
        AtomicReference<Throwable> holder = holder(exchange);
        return holder == null ? null : holder.get();
    }

    static void record(Exchange exchange, Throwable failure) {
        AtomicReference<Throwable> holder = holder(exchange);
        if (holder == null) {
            holder = new AtomicReference<>();
            exchange.setProperty(PlatformExchangeProperties.STEP_OUTCOME_FAILURE, holder);
        }
        holder.compareAndSet(null, failure);
    }

    @SuppressWarnings("unchecked")
    private static AtomicReference<Throwable> holder(Exchange exchange) {
        return exchange.getProperty(PlatformExchangeProperties.STEP_OUTCOME_FAILURE, AtomicReference.class);
    }
}
