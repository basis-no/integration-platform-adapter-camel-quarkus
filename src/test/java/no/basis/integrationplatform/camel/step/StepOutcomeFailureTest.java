package no.basis.integrationplatform.camel.step;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/** How another exchange's failure is recognised as a step outcome that was not recorded. */
class StepOutcomeFailureTest {
    private final DefaultCamelContext context = new DefaultCamelContext();

    @Test
    void anEqualButDifferentExceptionIsNotTheRecordedFailure() {
        var recorded = new EqualFailure();
        StepOutcomeFailure.record(exchange(), recorded);

        assertNull(StepOutcomeFailure.of(failedWith(new EqualFailure())));
        assertSame(recorded, StepOutcomeFailure.of(failedWith(recorded)));
    }

    @Test
    void aFailureIsFoundAtAnyDepthOfCausesAndSuppressedExceptions() {
        var recorded = new IllegalStateException("synthetic step outcome outage");
        StepOutcomeFailure.record(exchange(), recorded);
        Throwable wrapped = recorded;
        for (int i = 0; i < 200; i++) {
            wrapped = new RuntimeException("wrapper " + i, wrapped);
        }
        var crowded = new RuntimeException("caller");
        for (int i = 0; i < 200; i++) {
            crowded.addSuppressed(new RuntimeException("unrelated " + i));
        }
        crowded.addSuppressed(recorded);

        assertSame(recorded, StepOutcomeFailure.of(failedWith(wrapped)));
        assertSame(recorded, StepOutcomeFailure.of(failedWith(crowded)));
    }

    @Test
    void aFailedSendsExceptionCarriesTheFailureAlsoWhenItRefusesSuppressedExceptions() {
        var rejection = new RuntimeException("synthetic receiver rejection", null, false, false) { };
        Exchange send = failedWith(rejection);
        var recorded = new IllegalStateException("synthetic step outcome outage");
        StepOutcomeFailure.record(send, recorded);
        rejection.addSuppressed(recorded);

        assertSame(rejection, StepOutcomeFailure.of(failedWith(new RuntimeException("caller", rejection))));
    }

    @Test
    void aCyclicGraphWithoutTheFailureEndsTheSearch() {
        StepOutcomeFailure.record(exchange(), new IllegalStateException("synthetic step outcome outage"));
        var first = new RuntimeException("first");
        var second = new RuntimeException("second", first);
        first.addSuppressed(second);

        assertNull(StepOutcomeFailure.of(failedWith(second)));
    }

    private Exchange exchange() {
        return new DefaultExchange(context);
    }

    private Exchange failedWith(Throwable failure) {
        Exchange exchange = exchange();
        exchange.setException(failure);
        return exchange;
    }

    /** A business exception whose instances are all equal. */
    private static final class EqualFailure extends RuntimeException {
        EqualFailure() {
            super("business rule violated");
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof EqualFailure;
        }

        @Override
        public int hashCode() {
            return 1;
        }
    }
}
