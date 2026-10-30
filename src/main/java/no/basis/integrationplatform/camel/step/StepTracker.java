package no.basis.integrationplatform.camel.step;

import org.apache.camel.Exchange;

public interface StepTracker {
    void beforeSend(Exchange exchange);

    void recordDelivered(Exchange exchange);

    void recordSkipped(Exchange exchange);

    void recordFailed(Exchange exchange);

    void recordFailedAndRethrow(Exchange exchange) throws Exception;
}
