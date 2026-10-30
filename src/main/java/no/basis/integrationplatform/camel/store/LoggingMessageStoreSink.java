package no.basis.integrationplatform.camel.store;

import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingMessageStoreSink implements MessageStoreSink {
    private static final Logger LOG = LoggerFactory.getLogger(LoggingMessageStoreSink.class);

    @Override
    public void record(MessageAttemptResult result) {
        LOG.info("Message attempt result: messageId={} routeId={} status={} steps={}",
                result.messageId(), result.routeId(), result.status(), result.steps().size());
    }
}
