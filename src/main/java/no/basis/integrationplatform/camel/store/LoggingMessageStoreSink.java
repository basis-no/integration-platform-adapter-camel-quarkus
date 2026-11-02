package no.basis.integrationplatform.camel.store;

import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.StepStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Non-durable outcome sink intended for local diagnostics. */
public class LoggingMessageStoreSink implements MessageStoreSink {
    private static final Logger LOG = LoggerFactory.getLogger(LoggingMessageStoreSink.class);

    /** Creates the stateless logging sink. */
    public LoggingMessageStoreSink() {
    }

    @Override
    public void record(MessageAttemptResult result) {
        // Count outcomes separately: a completed replay whose steps were all skipped delivered nothing.
        LOG.info("Message attempt result: messageId={} routeId={} status={} steps={} delivered={} skipped={} failed={}",
                result.messageId(), result.routeId(), result.status(), result.steps().size(),
                count(result, StepStatus.DELIVERED), count(result, StepStatus.SKIPPED), count(result, StepStatus.FAILED));
    }

    private long count(MessageAttemptResult result, StepStatus status) {
        return result.steps().stream().filter(step -> step.status() == status).count();
    }
}
