package no.basis.integrationplatform.camel.step;

import no.basis.integrationplatform.camel.PlatformExchangeProperties;
import org.apache.camel.Exchange;
import org.apache.camel.support.SynchronizationAdapter;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The first failure to check or record a step outcome in a message's attempt, for example
 * during a Message Store outage. While it is set, the input is neither parked nor committed,
 * so it is consumed again. Split parts, loop iterations and branches run on copies of the
 * exchange, so the input route starts one holder that the copies share.
 *
 * <p>A separate exchange, such as a request sent with a {@code ProducerTemplate}, has its own
 * holder. Such an exchange ends failed when one of its step outcomes was not recorded, also
 * when its route caught the failure, and an exchange whose failure carries that failure, as
 * itself, a cause or a suppressed exception, counts as having it too. Work that ends after its
 * input completed, such as a {@code wireTap}, cannot hold that input back.</p>
 */
public final class StepOutcomeFailure {
    private static final int MAX_EXAMINED = 64;
    /** Recorded failures, held weakly, so that one passed on to another exchange is recognised there. */
    private static final Set<Throwable> RECORDED = Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<>()));

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
     * The attempt's first failure to check or record a step outcome: the one in its holder, or
     * one that the exchange's failure carries from another exchange.
     * @param exchange the input exchange or a copy of it
     * @return the failure, or {@code null} when every step outcome was checked and recorded
     */
    public static Throwable of(Exchange exchange) {
        AtomicReference<Throwable> holder = holder(exchange);
        Throwable failure = holder == null ? null : holder.get();
        if (failure == null) {
            failure = carried(exchange.getException());
        }
        if (failure == null) {
            failure = carried(exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class));
        }
        return failure;
    }

    static void record(Exchange exchange, Throwable failure) {
        RECORDED.add(failure);
        AtomicReference<Throwable> holder = holder(exchange);
        if (holder == null) {
            // No input route started this exchange: end it failed, so that whoever sent it sees the failure.
            holder = new AtomicReference<>();
            exchange.setProperty(PlatformExchangeProperties.STEP_OUTCOME_FAILURE, holder);
            exchange.getExchangeExtension().addOnCompletion(new FailOnCompletion(holder));
        }
        holder.compareAndSet(null, failure);
    }

    /** A recorded failure among {@code failure}, its causes and suppressed exceptions, examining a bounded number. */
    private static Throwable carried(Throwable failure) {
        if (failure == null || RECORDED.isEmpty()) {
            return null;
        }
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        ArrayDeque<Throwable> pending = new ArrayDeque<>();
        pending.add(failure);
        while (!pending.isEmpty() && seen.size() < MAX_EXAMINED) {
            Throwable next = pending.poll();
            if (!seen.add(next)) {
                continue;
            }
            if (RECORDED.contains(next)) {
                return next;
            }
            if (next.getCause() != null) {
                pending.add(next.getCause());
            }
            Collections.addAll(pending, next.getSuppressed());
        }
        return null;
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
