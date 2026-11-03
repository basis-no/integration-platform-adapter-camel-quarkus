package no.basis.integrationplatform.camel.step;

import no.basis.integrationplatform.camel.PlatformExchangeProperties;
import org.apache.camel.Exchange;
import org.apache.camel.support.SynchronizationAdapter;

import java.util.concurrent.atomic.AtomicReference;

/**
 * The first failure to check or record a step outcome in a message's attempt, for example
 * during a Message Store outage. While it is set, the input is neither parked nor committed,
 * so it is consumed again. Split parts, loop iterations and branches run on copies of the
 * exchange, so a managed Kafka input starts one holder that the copies share.
 *
 * <p>Any other exchange, such as a request sent with a {@code ProducerTemplate}, gets its own
 * holder when one of its step outcomes is not recorded, and ends failed: when it completes
 * without an exception, also because its route caught the failure, the failure becomes its
 * exception. An exception it already has is never changed. Such a failure does not hold back
 * the input that sent the request, and work that ends after its input completed, such as a
 * {@code wireTap}, cannot hold that input back either.</p>
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
            // No input started this exchange: it must not complete as if the outcome were recorded.
            holder = new AtomicReference<>();
            exchange.setProperty(PlatformExchangeProperties.STEP_OUTCOME_FAILURE, holder);
            exchange.getExchangeExtension().addOnCompletion(new FailOnCompletion(holder));
        }
        holder.compareAndSet(null, failure);
    }

    @SuppressWarnings("unchecked")
    private static AtomicReference<Throwable> holder(Exchange exchange) {
        return exchange.getProperty(PlatformExchangeProperties.STEP_OUTCOME_FAILURE, AtomicReference.class);
    }

    private static final class FailOnCompletion extends SynchronizationAdapter {
        private final AtomicReference<Throwable> holder;

        private FailOnCompletion(AtomicReference<Throwable> holder) {
            this.holder = holder;
        }

        @Override
        public void onComplete(Exchange exchange) {
            Throwable failure = holder.get();
            if (failure != null && exchange.getException() == null) {
                exchange.setException(failure);
            }
        }
    }
}
