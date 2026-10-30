package no.basis.integrationplatform.camel.store;

import no.basis.integrationplatform.camel.model.MessageAttemptResult;

/** Durable boundary for recording one terminal platform message attempt. */
public interface MessageStoreSink {
    /**
     * Records a terminal attempt before the adapter acknowledges its source.
     *
     * @param result terminal message attempt
     * @throws Exception when durable recording fails
     */
    void record(MessageAttemptResult result) throws Exception;
}
