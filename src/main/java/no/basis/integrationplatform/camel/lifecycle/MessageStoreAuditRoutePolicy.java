package no.basis.integrationplatform.camel.lifecycle;

import no.basis.integrationplatform.camel.PlatformExchangeProperties;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.store.MessageStoreSink;
import no.basis.integrationplatform.camel.store.MessageStoreSinkException;
import org.apache.camel.Exchange;
import org.apache.camel.Route;
import org.apache.camel.component.kafka.KafkaConstants;

/** Records independent delivery requests without Kafka recovery or replay ownership. */
public final class MessageStoreAuditRoutePolicy extends MessageStoreRoutePolicy {
    private static final String ELIGIBLE = "basis.messageStore.auditEligible";
    private static final String MESSAGE_ID = "basis.messageStore.auditMessageId";
    private static final String CORRELATION_ID = "basis.messageStore.auditCorrelationId";
    private static final String CONTENT_TYPE = "basis.messageStore.auditContentType";
    private final MessageStoreSink sink;

    public MessageStoreAuditRoutePolicy(MessageStoreSink sink, String integrationName,
            PayloadCapturePolicy payloadPolicy) {
        // Retention is independent of input replay enablement for explicitly selected audit routes.
        super(sink, integrationName, new PayloadCapturePolicy(
                payloadPolicy.mode(), payloadPolicy.maxBytes(), payloadPolicy.retention(), true));
        this.sink = sink;
    }

    @Override
    public void onExchangeBegin(Route route, Exchange exchange) {
        if (exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID) != null
                || exchange.getMessage().getHeader(PlatformHeaders.ATTEMPT) != null
                || exchange.getMessage().getHeader(PlatformHeaders.REPLAY) != null
                || exchange.getMessage().getHeader(KafkaConstants.MANUAL_COMMIT) != null
                || exchange.getProperty(PlatformExchangeProperties.KAFKA_MANUAL_COMMIT) != null
                || exchange.getProperty(PlatformExchangeProperties.PAYLOAD_STORAGE) != null) {
            var failure = new IllegalArgumentException("Audit route '" + route.getRouteId()
                    + "' requires an independent request exchange without input identity or Kafka commit state");
            exchange.setException(failure);
            exchange.setRouteStop(true);
            throw failure;
        }
        super.onExchangeBegin(route, exchange);
        byte[] captured = exchange.getProperty(PlatformExchangeProperties.PAYLOAD_BYTES, byte[].class);
        if (captured != null) {
            exchange.setProperty(PlatformExchangeProperties.PAYLOAD_BYTES, captured.clone());
        }
        exchange.setProperty(MESSAGE_ID, exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID));
        exchange.setProperty(CORRELATION_ID, exchange.getMessage().getHeader(PlatformHeaders.CORRELATION_ID));
        String contentType = exchange.getMessage().getHeader(PlatformHeaders.CONTENT_TYPE, String.class);
        if (contentType == null) {
            contentType = exchange.getMessage().getHeader(Exchange.CONTENT_TYPE, String.class);
        }
        exchange.setProperty(CONTENT_TYPE, contentType);
        exchange.setProperty(PlatformExchangeProperties.REPLAYABLE, false);
        exchange.setProperty(PlatformExchangeProperties.REPLAY_DISABLED_REASON,
                "delivery audit only; replay the original input instead");
        exchange.setProperty(ELIGIBLE, true);
    }

    @Override
    public void onExchangeDone(Route route, Exchange exchange) {
        if (!Boolean.TRUE.equals(exchange.getProperty(ELIGIBLE, Boolean.class))) {
            return;
        }
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, exchange.getProperty(MESSAGE_ID));
        exchange.getMessage().setHeader(PlatformHeaders.CORRELATION_ID, exchange.getProperty(CORRELATION_ID));
        exchange.getMessage().setHeader(PlatformHeaders.CONTENT_TYPE, exchange.getProperty(CONTENT_TYPE));
        exchange.removeProperty(PlatformExchangeProperties.DLQ_PARKED);
        try {
            sink.record(result(route, exchange));
        } catch (Exception failure) {
            var storeFailure = new MessageStoreSinkException("Failed to record delivery audit", failure);
            if (exchange.getException() != null) {
                exchange.getException().addSuppressed(storeFailure);
            } else {
                exchange.setException(storeFailure);
            }
            throw storeFailure;
        }
        // Never commit an offset, park a payload, handle a receiver failure, or change its exception.
    }
}
