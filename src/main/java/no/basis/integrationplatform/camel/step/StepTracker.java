package no.basis.integrationplatform.camel.step;

import org.apache.camel.Exchange;

/** Observes an intercepted endpoint send and records replay-relevant outcomes. */
public interface StepTracker {
    /**
     * Prepares classification, idempotency, and skip state before an endpoint send.
     *
     * @param exchange intercepted Camel exchange
     */
    void beforeSend(Exchange exchange);

    /**
     * Records a successful side-effect send.
     *
     * @param exchange intercepted Camel exchange
     */
    void recordDelivered(Exchange exchange);

    /**
     * Records that an already delivered side effect was not repeated.
     *
     * @param exchange intercepted Camel exchange
     */
    void recordSkipped(Exchange exchange);

    /**
     * Restores output needed after an output-producing side effect is skipped.
     *
     * @param exchange intercepted Camel exchange
     */
    default void restoreSkippedOutput(Exchange exchange) {
        // One-way side effects have no output to restore.
    }

    /**
     * Records a failed side-effect send.
     *
     * @param exchange intercepted Camel exchange
     */
    void recordFailed(Exchange exchange);

    /**
     * Records a failed side-effect send and rethrows the intercepted failure.
     *
     * @param exchange intercepted Camel exchange
     * @throws Exception original endpoint failure
     */
    void recordFailedAndRethrow(Exchange exchange) throws Exception;
}
