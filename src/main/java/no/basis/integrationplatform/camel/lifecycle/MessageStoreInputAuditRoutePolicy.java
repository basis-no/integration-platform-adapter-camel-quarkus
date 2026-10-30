package no.basis.integrationplatform.camel.lifecycle;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import no.basis.integrationplatform.camel.PlatformExchangeProperties;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.store.MessageStoreSink;
import no.basis.integrationplatform.camel.store.MessageStoreSinkException;
import org.apache.camel.Exchange;
import org.apache.camel.Route;
import org.apache.camel.component.kafka.KafkaConstants;

/** Inspects a non-Kafka input without assuming ownership of its acknowledgement or recovery. */
public final class MessageStoreInputAuditRoutePolicy extends MessageStoreRoutePolicy {
    private static final String SNAPSHOT = "basis.messageStore.inputAuditHeaders";
    private static final String ROW_ID = "basis.messageStore.inputAuditRowId";
    private static final String CAPTURE = "basis.messageStore.inputAuditCapture";
    private static final String REASON = "non-Kafka input inspection only; use source-specific recovery";
    private static final String[] HEADERS = { PlatformHeaders.CORRELATION_ID, PlatformHeaders.CONTENT_TYPE,
            PlatformHeaders.SOURCE_SYSTEM, PlatformHeaders.TARGET_SYSTEM, PlatformHeaders.SCHEMA_VERSION };
    private static final String[] PAYLOAD_PROPERTIES = { PlatformExchangeProperties.PAYLOAD_STORAGE,
            PlatformExchangeProperties.PAYLOAD_SIZE_BYTES, PlatformExchangeProperties.PAYLOAD_BYTES,
            PlatformExchangeProperties.PAYLOAD_EXPIRES_AT, PlatformExchangeProperties.PAYLOAD_DELETED_AT };
    private final MessageStoreSink sink;

    public MessageStoreInputAuditRoutePolicy(MessageStoreSink sink, String integrationName,
            PayloadCapturePolicy payloadPolicy) {
        super(sink, integrationName, new PayloadCapturePolicy(
                payloadPolicy.mode(), payloadPolicy.maxBytes(), payloadPolicy.retention(), true));
        this.sink = sink;
    }

    @Override
    public void onExchangeBegin(Route route, Exchange exchange) {
        if (exchange.getMessage().getHeader(PlatformHeaders.ATTEMPT) != null
                || exchange.getMessage().getHeader(PlatformHeaders.REPLAY) != null
                || exchange.getMessage().getHeader(KafkaConstants.MANUAL_COMMIT) != null
                || exchange.getProperty(PlatformExchangeProperties.KAFKA_MANUAL_COMMIT) != null
                || exchange.getProperty(PlatformExchangeProperties.PAYLOAD_STORAGE) != null
                || exchange.getProperty(SNAPSHOT) != null) {
            var failure = new IllegalArgumentException("Input inspection route '" + route.getRouteId()
                    + "' requires a fresh non-Kafka input without replay or inherited capture/commit state");
            exchange.setException(failure);
            exchange.setRouteStop(true);
            throw failure;
        }
        String rowId = UUID.randomUUID().toString();
        Exchange capture = exchange.copy();
        String inputId = exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID, String.class);
        capture.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, rowId);
        String correlation = exchange.getMessage().getHeader(PlatformHeaders.CORRELATION_ID, String.class);
        if (correlation == null || correlation.isBlank()) {
            capture.getMessage().setHeader(PlatformHeaders.CORRELATION_ID,
                    inputId == null || inputId.isBlank() ? rowId : inputId);
        }
        // Capturing a poll/batch must not seed one operational ID into all records
        // emitted by its split, or override the producer's recovery state.
        super.onExchangeBegin(route, capture);
        byte[] captured = capture.getProperty(PlatformExchangeProperties.PAYLOAD_BYTES, byte[].class);
        if (captured != null) {
            capture.setProperty(PlatformExchangeProperties.PAYLOAD_BYTES, captured.clone());
        }
        Map<String, Object> headers = new LinkedHashMap<>();
        for (String name : HEADERS) {
            Object value = capture.getMessage().getHeader(name);
            if (value != null) {
                headers.put(name, value);
            }
        }
        if (!headers.containsKey(PlatformHeaders.CONTENT_TYPE)) {
            String contentType = exchange.getMessage().getHeader(Exchange.CONTENT_TYPE, String.class);
            if (contentType != null) {
                headers.put(PlatformHeaders.CONTENT_TYPE, contentType);
            }
        }
        Map<String, Object> payload = new LinkedHashMap<>();
        for (String name : PAYLOAD_PROPERTIES) {
            Object value = capture.getProperty(name);
            if (value != null) {
                payload.put(name, value);
            }
        }
        exchange.setProperty(SNAPSHOT, Map.copyOf(headers));
        exchange.setProperty(ROW_ID, rowId);
        exchange.setProperty(CAPTURE, Map.copyOf(payload));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void onExchangeDone(Route route, Exchange exchange) {
        String rowId = exchange.getProperty(ROW_ID, String.class);
        if (rowId == null) {
            return;
        }
        // A producer may assign different IDs/headers to the messages it emits.
        // Record the input snapshot without changing those operational headers.
        Exchange inspection = exchange.copy();
        Map<String, Object> headers = exchange.getProperty(SNAPSHOT, Map.class);
        for (String name : HEADERS) {
            inspection.getMessage().setHeader(name, headers.get(name));
        }
        Map<String, Object> payload = exchange.getProperty(CAPTURE, Map.class);
        for (String name : PAYLOAD_PROPERTIES) {
            inspection.setProperty(name, payload.get(name));
        }
        inspection.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, rowId);
        inspection.getMessage().removeHeader(PlatformHeaders.ATTEMPT);
        inspection.getMessage().removeHeader(PlatformHeaders.REPLAY);
        inspection.removeProperty(PlatformExchangeProperties.DLQ_PARKED);
        inspection.setProperty(PlatformExchangeProperties.REPLAYABLE, false);
        inspection.setProperty(PlatformExchangeProperties.REPLAY_DISABLED_REASON, REASON);
        try {
            sink.record(result(route, inspection));
        } catch (Exception failure) {
            var storeFailure = new MessageStoreSinkException("Failed to record non-Kafka input inspection", failure);
            if (exchange.getException() != null) {
                exchange.getException().addSuppressed(storeFailure);
            } else {
                exchange.setException(storeFailure);
            }
            throw storeFailure;
        }
    }
}
