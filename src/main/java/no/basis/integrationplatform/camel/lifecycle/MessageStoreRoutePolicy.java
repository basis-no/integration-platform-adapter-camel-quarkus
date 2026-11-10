package no.basis.integrationplatform.camel.lifecycle;

import no.basis.integrationplatform.camel.KafkaRecordHeaders;
import no.basis.integrationplatform.camel.PlatformExchangeProperties;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.step.DefaultStepTracker;
import no.basis.integrationplatform.camel.step.StepOutcomeFailure;
import no.basis.integrationplatform.camel.store.MessageStoreSink;
import no.basis.integrationplatform.camel.traffic.TrafficSignals;
import org.apache.camel.Exchange;
import org.apache.camel.Route;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.support.RoutePolicySupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class MessageStoreRoutePolicy extends RoutePolicySupport {
    private static final Logger LOG = LoggerFactory.getLogger(MessageStoreRoutePolicy.class);
    private static final String INPUT_CORRELATION_ID = "basis.messageStore.inputCorrelationId";
    private static final String ENVELOPE_PREFIX = "x-bip-";
    private static final String INPUT_ENVELOPE = "basis.messageStore.inputEnvelope";
    private static final List<String> ENVELOPE_HEADERS = List.of(PlatformHeaders.ATTEMPT, PlatformHeaders.SCHEMA_VERSION,
            PlatformHeaders.CONTENT_TYPE, PlatformHeaders.SOURCE_SYSTEM, PlatformHeaders.TARGET_SYSTEM);

    private final MessageStoreSink sink;
    private TrafficSignals trafficSignals;
    private final String integrationName;
    private final PayloadCapturePolicy payloadPolicy;
    // Per topic partition: the offset of the record held back last, and how often in a row.
    private final Map<String, long[]> heldBack = new ConcurrentHashMap<>();
    private Sleeper sleeper = MessageStoreRoutePolicy::sleepUnlessStopping;

    static final long FIRST_BACK_OFF_MS = 1_000;
    static final long MAX_BACK_OFF_MS = 30_000;
    private static final long DEFAULT_MAX_POLL_INTERVAL_MS = 300_000;
    private static final int MAX_HELD_BACK_PARTITIONS = 1_024;
    private static final String RECORD_POSITION = "basis.messageStore.recordPosition";
    private static final String POLL_STARTED = "basis.messageStore.pollStarted";
    private static final String POLL_THREAD = "basis.messageStore.pollThread";
    private static final String LAST_POLL_RECORD = "basis.messageStore.lastPollRecord";
    // Per consumer thread: when its current poll's records began, and whether the next record
    // starts a new poll. A failed record ends its poll: with breakOnFirstError the consumer
    // stops the batch and reconnects.
    private final Map<Long, Long> pollStarted = new ConcurrentHashMap<>();
    private final Set<Long> newPollNext = ConcurrentHashMap.newKeySet();

    /** Waits before a record is handed back to its consumer to be read again. */
    @FunctionalInterface
    interface Sleeper {
        void sleep(Route route, long millis);
    }

    public MessageStoreRoutePolicy(MessageStoreSink sink, String integrationName) {
        this(sink, integrationName, PayloadCapturePolicy.defaultPolicy());
    }

    public MessageStoreRoutePolicy(MessageStoreSink sink, String integrationName, PayloadCapturePolicy payloadPolicy) {
        this.sink = sink;
        this.integrationName = integrationName;
        this.payloadPolicy = payloadPolicy;
    }

    void setTrafficSignals(TrafficSignals trafficSignals) {
        this.trafficSignals = trafficSignals;
    }

    void setSleeper(Sleeper sleeper) {
        this.sleeper = sleeper;
    }

    @Override
    public void onExchangeBegin(Route route, Exchange exchange) {
        normalizeKafkaEnvelopeHeaders(exchange);
        // A replay's redo list is for this attempt only; the route must not forward it.
        DefaultStepTracker.captureRedoSteps(exchange);
        exchange.setProperty(PlatformExchangeProperties.INPUT_MESSAGE_ID, ensureMessageId(exchange));
        exchange.setProperty(INPUT_CORRELATION_ID, exchange.getMessage().getHeader(PlatformHeaders.CORRELATION_ID, String.class));
        captureInputEnvelope(exchange);
        captureKafkaManualCommit(exchange);
        if (exchange.getProperty(PlatformExchangeProperties.KAFKA_MANUAL_COMMIT) != null) {
            // Only an input the adapter parks and commits is held back by its attempt's step outcomes.
            StepOutcomeFailure.begin(exchange);
            captureRecordPosition(exchange);
            capturePollStart(exchange);
        }
        capturePayloadMetadata(exchange);
        if (trafficSignals != null) trafficSignals.recordAdmitted(exchange);
    }

    @Override
    public void onExchangeDone(Route route, Exchange exchange) {
        MessageAttemptResult result = result(route, exchange);

        try {
            sink.record(result);
        } catch (Exception e) {
            readAgain(route, exchange, "Failed to record message outcome; Kafka offset was not committed", e);
            return;
        }
        Throwable unrecorded = StepOutcomeFailure.of(exchange);
        if (unrecorded != null) {
            readAgain(route, exchange, "A step outcome was not checked or recorded; Kafka offset was not committed",
                    unrecorded);
            return;
        }
        if (result.status() == MessageStatus.FAILED) {
            readAgain(route, exchange, "Processing failed without confirmed DLQ parking; Kafka offset was not committed",
                    null);
            return;
        }
        try {
            commitKafkaOffsetIfPresent(exchange);
        } catch (Exception e) {
            fail(exchange, "Message outcome was recorded but Kafka offset commit failed", e);
        }
        String position = exchange.getProperty(RECORD_POSITION, String.class);
        if (position != null) {
            heldBack.remove(partition(position));
        }
        if (Boolean.TRUE.equals(exchange.getProperty(LAST_POLL_RECORD, Boolean.class))) {
            endOfPoll(exchange);
        }
    }

    /** The outcome was not made durable: the record is not committed and is read again. */
    private void readAgain(Route route, Exchange exchange, String message, Throwable cause) {
        backOffBeforeReadingAgain(route, exchange);
        fail(exchange, message, cause);
    }

    private void fail(Exchange exchange, String message, Throwable cause) {
        var failure = new IllegalStateException(message, cause);
        // Camel logs/ignores exceptions thrown by route-policy completion. Keep
        // the exchange failed as well, so the Kafka consumer can seek/retry.
        exchange.setException(failure);
        throw failure;
    }

    protected MessageAttemptResult result(Route route, Exchange exchange) {
        boolean dlqParked = dlqParked(exchange);
        Throwable failure = failure(exchange, dlqParked);
        // The input is consumed again, also when a route handled the failure or a copy was parked.
        Throwable unrecorded = StepOutcomeFailure.of(exchange);
        MessageStatus status = unrecorded != null ? MessageStatus.FAILED : status(dlqParked, failure);
        if (failure == null) {
            failure = unrecorded;
        }

        return new MessageAttemptResult(
                schemaVersion(exchange),
                messageId(exchange),
                correlationId(exchange),
                envelope(exchange, PlatformHeaders.CONTENT_TYPE, String.class),
                envelope(exchange, PlatformHeaders.SOURCE_SYSTEM, String.class),
                envelope(exchange, PlatformHeaders.TARGET_SYSTEM, String.class),
                payloadStorage(exchange),
                payloadSizeBytes(exchange),
                payloadBytes(exchange),
                payloadExpiresAt(exchange),
                payloadDeletedAt(exchange),
                replayable(exchange),
                replayDisabledReason(exchange),
                integrationName,
                route.getRouteId(),
                attempt(exchange),
                autoRedelivered(exchange),
                autoRedeliveryCount(exchange),
                autoRedeliveryMax(exchange),
                Instant.now(),
                status,
                steps(exchange),
                errorMessage(failure));
    }

    private MessageStatus status(boolean dlqParked, Throwable failure) {
        if (dlqParked) {
            return MessageStatus.DLQ;
        }
        return failure == null ? MessageStatus.COMPLETED : MessageStatus.FAILED;
    }

    @SuppressWarnings("unchecked")
    private List<StepResult> steps(Exchange exchange) {
        List<StepResult> steps = exchange.getProperty(PlatformExchangeProperties.STEPS, List.class);
        return steps == null ? List.of() : List.copyOf(steps);
    }

    private void commitKafkaOffsetIfPresent(Exchange exchange) {
        KafkaManualCommit manualCommit = exchange.getProperty(
                PlatformExchangeProperties.KAFKA_MANUAL_COMMIT,
                KafkaManualCommit.class);
        if (manualCommit == null) {
            manualCommit = exchange.getMessage().getHeader(KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class);
        }
        if (manualCommit == null) {
            return;
        }

        manualCommit.commit();
        LOG.debug("Committed Kafka offset for message {}", exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID));
    }

    /**
     * A Kafka record whose outcome was not made durable is read again, and its partition
     * waits. The first time it is read again at once, so a passing glitch costs nothing; after
     * that each read waits 1 s, doubling, at most 30 s, so that a Message Store outage or a DLQ
     * that refuses the record does not run the route, and its sends, several times a second.
     * The wait is also kept to half the consumer's {@code max.poll.interval.ms} less the time
     * since the poll that returned this record began to be processed, so the records before it
     * in that poll count. Without {@code breakOnFirstError}, where a failed record does not end
     * its poll, there is no wait. The consumer waits for it, also when the policy runs on a
     * completion thread, and the wait ends early when the context stops. A committed outcome on
     * the partition resets it. A record without its position is read again at once, as before.
     */
    private void backOffBeforeReadingAgain(Route route, Exchange exchange) {
        String position = exchange.getProperty(RECORD_POSITION, String.class);
        // The failed record ends its poll, so the next record on this thread starts a new one.
        endOfPoll(exchange);
        if (position == null || !breaksOnFirstError(route)) {
            return;
        }
        long offset = Long.parseLong(position.substring(position.lastIndexOf('@') + 1));
        if (heldBack.size() >= MAX_HELD_BACK_PARTITIONS && !heldBack.containsKey(partition(position))) {
            // Partitions that moved away or whose topic went keep no state for ever.
            heldBack.keySet().stream().findFirst().ifPresent(heldBack::remove);
        }
        long[] state = heldBack.compute(partition(position),
                (key, previous) -> previous != null && previous[0] == offset
                        ? new long[]{offset, previous[1] + 1} : new long[]{offset, 1});
        if (state[1] < 2) {
            return;
        }
        // The poll interval runs from the poll, so the records before this one in the poll count.
        Long started = exchange.getProperty(POLL_STARTED, Long.class);
        long elapsed = Math.max(0, System.currentTimeMillis()
                - (started != null ? started : exchange.getClock().getCreated()));
        long wait = Math.min(Math.min(MAX_BACK_OFF_MS, FIRST_BACK_OFF_MS << Math.min(state[1] - 2, 16)),
                maxPollIntervalMs(route) / 2 - elapsed);
        if (wait <= 0) {
            return;
        }
        LOG.info("Message {} at {} could not be made durable {} times in a row; reading it again in {} ms",
                exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID), position, state[1], wait);
        sleeper.sleep(route, wait);
    }

    private void capturePollStart(Exchange exchange) {
        long thread = Thread.currentThread().getId();
        long now = System.currentTimeMillis();
        if (newPollNext.remove(thread) || !pollStarted.containsKey(thread)) {
            pollStarted.put(thread, now);
        }
        exchange.setProperty(POLL_STARTED, pollStarted.getOrDefault(thread, now));
        exchange.setProperty(POLL_THREAD, thread);
        Object last = exchange.getMessage().getHeader("CamelKafkaLastPollRecord");
        if (last == null) {
            last = exchange.getMessage().getHeader("kafka.LAST_POLL_RECORD");
        }
        exchange.setProperty(LAST_POLL_RECORD, Boolean.TRUE.equals(last) || "true".equals(String.valueOf(last)));
    }

    private void endOfPoll(Exchange exchange) {
        Long thread = exchange.getProperty(POLL_THREAD, Long.class);
        if (thread != null) {
            newPollNext.add(thread);
        }
    }

    private static boolean breaksOnFirstError(Route route) {
        try {
            if (route.getEndpoint() instanceof org.apache.camel.component.kafka.KafkaEndpoint kafka) {
                return kafka.getConfiguration().isBreakOnFirstError();
            }
        } catch (RuntimeException unavailable) {
            // Not a Kafka route the policy can inspect: the adapter's default applies.
        }
        return true;
    }

    private static String partition(String position) {
        return position.substring(0, position.lastIndexOf('@'));
    }

    /** The consumer's effective poll interval: an additional property overrides the named option. */
    static long maxPollIntervalMs(Route route) {
        try {
            if (route.getEndpoint() instanceof org.apache.camel.component.kafka.KafkaEndpoint kafka) {
                var configuration = kafka.getConfiguration();
                Object override = configuration.getAdditionalProperties() == null ? null
                        : configuration.getAdditionalProperties().get("max.poll.interval.ms");
                if (override != null) {
                    long value = Long.parseLong(override.toString().trim());
                    if (value > 0) {
                        return value;
                    }
                }
                Integer configured = configuration.getMaxPollIntervalMs();
                if (configured != null && configured > 0) {
                    return configured;
                }
            }
        } catch (RuntimeException unavailable) {
            // Not a Kafka route the policy can inspect: Kafka's default applies.
        }
        return DEFAULT_MAX_POLL_INTERVAL_MS;
    }

    static void sleepUnlessStopping(Route route, long millis) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
        try {
            for (long left = millis; left > 0; left = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())) {
                if (route.getCamelContext() != null && route.getCamelContext().isStopping()) {
                    return;
                }
                Thread.sleep(Math.min(100, left));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void captureRecordPosition(Exchange exchange) {
        Object topic = KafkaRecordHeaders.topic(exchange.getMessage());
        Object partition = KafkaRecordHeaders.partition(exchange.getMessage());
        Object offset = KafkaRecordHeaders.offset(exchange.getMessage());
        if (topic instanceof String name && partition instanceof Number p && offset instanceof Number o) {
            exchange.setProperty(RECORD_POSITION, name + "/" + p.intValue() + "@" + o.longValue());
        }
    }

    private void captureKafkaManualCommit(Exchange exchange) {
        KafkaManualCommit manualCommit = exchange.getMessage().getHeader(KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class);
        if (manualCommit != null) {
            exchange.setProperty(PlatformExchangeProperties.KAFKA_MANUAL_COMMIT, manualCommit);
        }
    }

    private void capturePayloadMetadata(Exchange exchange) {
        Object body = exchange.getMessage().getBody();
        byte[] bytes = payloadBytes(body);
        Long sizeBytes = bytes == null ? payloadSizeBytes(body) : Long.valueOf(bytes.length);
        if (sizeBytes != null) {
            exchange.setProperty(PlatformExchangeProperties.PAYLOAD_SIZE_BYTES, sizeBytes);
        }

        String metadataOnlyReason = metadataOnlyReason(bytes);
        if (metadataOnlyReason != null) {
            metadataOnly(exchange, metadataOnlyReason);
            keepForParking(exchange, body);
            return;
        }

        exchange.setProperty(PlatformExchangeProperties.PAYLOAD_STORAGE, PayloadMode.POSTGRES_INLINE.storageValue());
        // A route may change a byte array body in place; replay and parking keep the consumed input.
        exchange.setProperty(PlatformExchangeProperties.PAYLOAD_BYTES, body instanceof byte[] ? bytes.clone() : bytes);
        exchange.setProperty(PlatformExchangeProperties.PAYLOAD_EXPIRES_AT,
                Instant.now().plus(payloadPolicy.retention()));
        exchange.setProperty(PlatformExchangeProperties.REPLAYABLE, true);
    }

    private String metadataOnlyReason(byte[] bytes) {
        if (!payloadPolicy.replayEnabled()) {
            return "replay disabled by policy";
        }
        if (payloadPolicy.mode() == PayloadMode.METADATA_ONLY) {
            return "payload storage mode is metadata-only";
        }
        if (bytes == null) {
            return "payload body type is not safely capturable";
        }
        if (bytes.length > payloadPolicy.maxBytes()) {
            return "payload exceeds max inline size";
        }
        return null;
    }

    /**
     * DLQ parking sends the consumed input also when replay storage does not keep it. Only a
     * managed Kafka input is parked, so other inputs keep no copy. A String cannot change and
     * is kept as it is; another body type is parked as the route left it.
     */
    private void keepForParking(Exchange exchange, Object body) {
        if (exchange.getProperty(PlatformExchangeProperties.KAFKA_MANUAL_COMMIT) == null) {
            return;
        }
        if (body == null) {
            exchange.setProperty(PlatformExchangeProperties.PARKING_PAYLOAD, new byte[0]);
        } else if (body instanceof byte[] bytes) {
            exchange.setProperty(PlatformExchangeProperties.PARKING_PAYLOAD, bytes.clone());
        } else if (body instanceof String) {
            exchange.setProperty(PlatformExchangeProperties.PARKING_PAYLOAD, body);
        }
    }

    private void metadataOnly(Exchange exchange, String reason) {
        exchange.setProperty(PlatformExchangeProperties.PAYLOAD_STORAGE, PayloadMode.METADATA_ONLY.storageValue());
        exchange.removeProperty(PlatformExchangeProperties.PAYLOAD_BYTES);
        exchange.removeProperty(PlatformExchangeProperties.PAYLOAD_EXPIRES_AT);
        exchange.setProperty(PlatformExchangeProperties.REPLAYABLE, false);
        exchange.setProperty(PlatformExchangeProperties.REPLAY_DISABLED_REASON, reason);
    }

    private Long payloadSizeBytes(Object body) {
        if (body == null) {
            return 0L;
        }
        if (body instanceof byte[] bytes) {
            return (long) bytes.length;
        }
        if (body instanceof String text) {
            return (long) text.getBytes(StandardCharsets.UTF_8).length;
        }
        return null;
    }

    private byte[] payloadBytes(Object body) {
        if (body == null) {
            return new byte[0];
        }
        if (body instanceof byte[] bytes) {
            return bytes;
        }
        if (body instanceof String text) {
            return text.getBytes(StandardCharsets.UTF_8);
        }
        return null;
    }

    // A route or aggregation strategy can replace the message without the x-bip-* headers
    // (for example a grouped split). The outcome still belongs to the consumed input.
    private String messageId(Exchange exchange) {
        String current = exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID, String.class);
        String input = exchange.getProperty(PlatformExchangeProperties.INPUT_MESSAGE_ID, String.class);
        if ((current == null || current.isBlank()) && input != null) {
            exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, input);
        }
        return ensureMessageId(exchange);
    }

    // A replay through a route that drops headers must stay its own attempt: falling back
    // to attempt 1 would overwrite the original attempt's history row.
    private void captureInputEnvelope(Exchange exchange) {
        Map<String, Object> envelope = new HashMap<>();
        for (String name : ENVELOPE_HEADERS) {
            Object value = exchange.getMessage().getHeader(name);
            if (value != null) {
                envelope.put(name, value);
            }
        }
        exchange.setProperty(INPUT_ENVELOPE, Map.copyOf(envelope));
    }

    @SuppressWarnings("unchecked")
    private <T> T envelope(Exchange exchange, String name, Class<T> type) {
        T current = exchange.getMessage().getHeader(name, type);
        if (current != null) {
            return current;
        }
        Map<String, Object> input = exchange.getProperty(INPUT_ENVELOPE, Map.class);
        Object captured = input == null ? null : input.get(name);
        return captured == null ? null : exchange.getContext().getTypeConverter().convertTo(type, exchange, captured);
    }

    private String correlationId(Exchange exchange) {
        String current = exchange.getMessage().getHeader(PlatformHeaders.CORRELATION_ID, String.class);
        return current != null ? current : exchange.getProperty(INPUT_CORRELATION_ID, String.class);
    }

    private String ensureMessageId(Exchange exchange) {
        String messageId = exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID, String.class);
        if (messageId != null && !messageId.isBlank()) {
            return messageId;
        }

        messageId = UUID.randomUUID().toString();
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, messageId);
        return messageId;
    }

    private void normalizeKafkaEnvelopeHeaders(Exchange exchange) {
        // Kafka exposes bytes, but the x-bip-* headers are UTF-8 text. An HTTP component
        // would send a byte array as its list of numbers, so keep the owned headers as text,
        // independent of the payload charset. A Kafka producer sends the same bytes again.
        if (KafkaRecordHeaders.topic(exchange.getMessage()) == null) {
            return;
        }
        for (String name : List.copyOf(exchange.getMessage().getHeaders().keySet())) {
            if (name.regionMatches(true, 0, ENVELOPE_PREFIX, 0, ENVELOPE_PREFIX.length())
                    && exchange.getMessage().getHeader(name) instanceof byte[] bytes) {
                exchange.getMessage().setHeader(name, new String(bytes, StandardCharsets.UTF_8));
            }
        }
    }

    private int schemaVersion(Exchange exchange) {
        Integer version = envelope(exchange, PlatformHeaders.SCHEMA_VERSION, Integer.class);
        return version == null ? 1 : version;
    }

    private int attempt(Exchange exchange) {
        Integer attempt = envelope(exchange, PlatformHeaders.ATTEMPT, Integer.class);
        return attempt == null || attempt < 1 ? 1 : attempt;
    }

    // Retry diagnostics. The redelivery headers survive on the
    // retry-then-success path but are stripped by Camel's error handler before
    // a dead-letter delivery, so the properties captured during redelivery
    // (PlatformRouteConfiguration#captureRedeliveryDiagnostics) take precedence.

    private boolean autoRedelivered(Exchange exchange) {
        if (Boolean.TRUE.equals(exchange.getProperty(PlatformExchangeProperties.AUTO_REDELIVERED, Boolean.class))) {
            return true;
        }
        Boolean redelivered = exchange.getMessage().getHeader(Exchange.REDELIVERED, Boolean.class);
        Integer count = autoRedeliveryCount(exchange);
        return Boolean.TRUE.equals(redelivered) || (count != null && count > 0);
    }

    private Integer autoRedeliveryCount(Exchange exchange) {
        Integer captured = exchange.getProperty(PlatformExchangeProperties.AUTO_REDELIVERY_COUNT, Integer.class);
        if (captured != null) {
            return captured;
        }
        return exchange.getMessage().getHeader(Exchange.REDELIVERY_COUNTER, Integer.class);
    }

    private Integer autoRedeliveryMax(Exchange exchange) {
        Integer captured = exchange.getProperty(PlatformExchangeProperties.AUTO_REDELIVERY_MAX, Integer.class);
        if (captured != null) {
            return captured;
        }
        return exchange.getMessage().getHeader(Exchange.REDELIVERY_MAX_COUNTER, Integer.class);
    }

    private String payloadStorage(Exchange exchange) {
        String storage = exchange.getProperty(PlatformExchangeProperties.PAYLOAD_STORAGE, String.class);
        return storage == null || storage.isBlank() ? "metadata_only" : storage;
    }

    private Long payloadSizeBytes(Exchange exchange) {
        return exchange.getProperty(PlatformExchangeProperties.PAYLOAD_SIZE_BYTES, Long.class);
    }

    private byte[] payloadBytes(Exchange exchange) {
        return exchange.getProperty(PlatformExchangeProperties.PAYLOAD_BYTES, byte[].class);
    }

    private Instant payloadExpiresAt(Exchange exchange) {
        return exchange.getProperty(PlatformExchangeProperties.PAYLOAD_EXPIRES_AT, Instant.class);
    }

    private Instant payloadDeletedAt(Exchange exchange) {
        return exchange.getProperty(PlatformExchangeProperties.PAYLOAD_DELETED_AT, Instant.class);
    }

    private boolean replayable(Exchange exchange) {
        return Boolean.TRUE.equals(exchange.getProperty(PlatformExchangeProperties.REPLAYABLE, Boolean.class));
    }

    private String replayDisabledReason(Exchange exchange) {
        return exchange.getProperty(PlatformExchangeProperties.REPLAY_DISABLED_REASON, String.class);
    }

    private Throwable failure(Exchange exchange, boolean includeCaughtException) {
        if (exchange.getException() != null) {
            return exchange.getException();
        }
        if (includeCaughtException || exchange.isFailed()) {
            return exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
        }
        return null;
    }

    private boolean dlqParked(Exchange exchange) {
        return Boolean.TRUE.equals(exchange.getProperty(PlatformExchangeProperties.DLQ_PARKED, Boolean.class));
    }

    private String errorMessage(Throwable failure) {
        if (failure == null) {
            return null;
        }
        return failure.getMessage() == null ? failure.getClass().getName() : failure.getMessage();
    }
}
