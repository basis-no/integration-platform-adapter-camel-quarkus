package no.basis.integrationplatform.camel.step;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/** A step outcome failure belongs to one attempt: the input exchange and its copies. */
class StepOutcomeFailureTest {
    private final DefaultCamelContext context = new DefaultCamelContext();

    @Test
    void copiesOfTheInputShareItsAttemptsFailure() {
        Exchange input = new DefaultExchange(context);
        StepOutcomeFailure.begin(input);
        Exchange part = input.copy();
        var failure = new IllegalStateException("synthetic step outcome outage");

        StepOutcomeFailure.record(part, failure);

        assertSame(failure, StepOutcomeFailure.of(input));
        assertSame(failure, StepOutcomeFailure.of(input.copy()));
    }

    @Test
    void anotherExchangeIsNotHeldBackByTheSameExceptionInstance() {
        // Some libraries throw one exception instance for every failure of a kind.
        var shared = new RuntimeException("connection closed");
        var sharedWithoutSuppression = new RuntimeException("connection reset", null, false, false) { };
        for (var receiverFailure : List.of(shared, sharedWithoutSuppression)) {
            Exchange send = failedWith(receiverFailure);
            var failure = new IllegalStateException("synthetic step outcome outage");
            StepOutcomeFailure.record(send, failure);

            assertSame(failure, StepOutcomeFailure.of(send));
            assertSame(receiverFailure, send.getException());
            assertEquals(0, receiverFailure.getSuppressed().length);
            assertNull(StepOutcomeFailure.of(failedWith(receiverFailure)));
            assertNull(StepOutcomeFailure.of(failedWith(new RuntimeException("caller", failure))),
                    "Only the attempt that failed to record is held back");
        }
    }

    private Exchange failedWith(Throwable failure) {
        Exchange exchange = new DefaultExchange(context);
        exchange.setException(failure);
        return exchange;
    }
}
