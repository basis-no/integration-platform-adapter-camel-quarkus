package no.basis.integrationplatform.camel.store;

import no.basis.integrationplatform.camel.model.MessageAttemptResult;

public interface MessageStoreSink {
    void record(MessageAttemptResult result) throws Exception;
}
