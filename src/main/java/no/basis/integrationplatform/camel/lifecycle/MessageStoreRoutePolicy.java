package no.basis.integrationplatform.camel.lifecycle;

import no.basis.integrationplatform.camel.KafkaRecordHeaders;
import no.basis.integrationplatform.camel.PlatformDeadLetterRoute;
import no.basis.integrationplatform.camel.PlatformExchangeProperties;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.payload.PayloadObjectStore;
import no.basis.integrationplatform.camel.payload.PayloadReference;
import no.basis.integrationplatform.camel.payload.PayloadStorageException;
import no.basis.integrationplatform.camel.step.DefaultStepTracker;
import no.basis.integrationplatform.camel.step.StepOutcomeFailure;
import no.basis.integrationplatform.camel.store.MessageStoreSink;
import no.basis.integrationplatform.camel.traffic.TrafficSignals;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePropertyKey;
import org.apache.camel.Route;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.support.EndpointHelper;
import org.apache.camel.support.RoutePolicySupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/** Records terminal lifecycle outcomes and owns the Kafka acknowledgement boundary. */
public class MessageStoreRoutePolicy extends RoutePolicySupport {
    private static final Logger LOG = LoggerFactory.getLogger(MessageStoreRoutePolicy.class);
    private static final String INPUT_CORRELATION_ID = "basis.messageStore.inputCorrelationId";
    private static final String CHECKED_COPY = "basis.messageStore.checkedPayloadCopy";
    private static final String ENVELOPE_PREFIX = "x-bip-";
    private static final String INPUT_ENVELOPE = "basis.messageStore.inputEnvelope";
    private static final List<String> ENVELOPE_HEADERS = List.of(PlatformHeaders.ATTEMPT, PlatformHeaders.SCHEMA_VERSION,
            PlatformHeaders.CONTENT_TYPE, PlatformHeaders.SOURCE_SYSTEM, PlatformHeaders.TARGET_SYSTEM);

    private final MessageStoreSink sink;
    private final String integrationName;
    private final PayloadCapturePolicy payloadPolicy;
    private final MessageBoundaryKind boundaryKind;
    private final boolean kafkaCommitEnabled;
    private final PayloadObjectStore objectStore;
    private TrafficSignals trafficSignals;
    private VerifiedPayloadSpool payloadSpool = VerifiedPayloadSpool.defaults();
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

    /**
     * Creates a Kafka policy using the default payload policy.
     *
     * @param sink terminal outcome sink
     * @param integrationName stable integration name
     */
    public MessageStoreRoutePolicy(MessageStoreSink sink, String integrationName) {
        this(sink, integrationName, PayloadCapturePolicy.defaultPolicy(), MessageBoundaryKind.KAFKA, true,
                PayloadObjectStore.disabled());
    }

    /**
     * Creates a Kafka policy using an explicit payload policy.
     *
     * @param sink terminal outcome sink
     * @param integrationName stable integration name
     * @param payloadPolicy payload capture and replay policy
     */
    public MessageStoreRoutePolicy(MessageStoreSink sink, String integrationName, PayloadCapturePolicy payloadPolicy) {
        this(sink, integrationName, payloadPolicy, MessageBoundaryKind.KAFKA, true, PayloadObjectStore.disabled());
    }

    MessageStoreRoutePolicy(
            MessageStoreSink sink,
            String integrationName,
            PayloadCapturePolicy payloadPolicy,
            MessageBoundaryKind boundaryKind) {
        this(sink, integrationName, payloadPolicy, boundaryKind, true, PayloadObjectStore.disabled());
    }

    MessageStoreRoutePolicy(
            MessageStoreSink sink,
            String integrationName,
            PayloadCapturePolicy payloadPolicy,
            MessageBoundaryKind boundaryKind,
            boolean kafkaCommitEnabled) {
        this(sink, integrationName, payloadPolicy, boundaryKind, kafkaCommitEnabled, PayloadObjectStore.disabled());
    }

    MessageStoreRoutePolicy(
            MessageStoreSink sink,
            String integrationName,
            PayloadCapturePolicy payloadPolicy,
            MessageBoundaryKind boundaryKind,
            boolean kafkaCommitEnabled,
            PayloadObjectStore objectStore) {
        this.sink = sink;
        this.integrationName = integrationName;
        this.payloadPolicy = payloadPolicy;
        this.boundaryKind = boundaryKind;
        this.kafkaCommitEnabled = kafkaCommitEnabled;
        this.objectStore = objectStore;
    }

    void setPayloadSpool(VerifiedPayloadSpool payloadSpool) {
        this.payloadSpool = payloadSpool;
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
        if (boundaryKind == MessageBoundaryKind.KAFKA) {
            captureKafkaManualCommit(exchange);
            captureRecordPosition(exchange);
            capturePollStart(exchange);
        }
        if (exchange.getProperty(PlatformExchangeProperties.KAFKA_MANUAL_COMMIT) != null) {
            // Only an input the adapter parks and commits is held back by its attempt's step outcomes.
            StepOutcomeFailure.begin(exchange);
        }
        try {
            capturePayloadMetadata(exchange);
        } catch (PayloadStorageException e) {
            failBeforeTheRoute(exchange, e);
        } catch (RuntimeException e) {
            // An application's own PayloadObjectStore, or a malformed reference, may throw anything.
            failBeforeTheRoute(exchange, new PayloadStorageException(
                    "The payload could not be captured or checked: " + e.getClass().getSimpleName(), e));
        }
        if (trafficSignals != null && boundaryKind == MessageBoundaryKind.KAFKA) {
            trafficSignals.recordAdmitted(exchange);
        }
    }

    /**
     * Camel ignores an exception thrown from {@code onExchangeBegin}, and runs the route. A
     * payload that could not be captured or checked must not reach it: the exchange fails and
     * stops, so no processor runs and nothing is parked, and its outcome is recorded FAILED
     * without a commit, so the record is read again.
     */
    private void failBeforeTheRoute(Exchange exchange, PayloadStorageException failure) {
        LOG.error("Message {} did not reach its route: {}{}",
                exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID), failure.getMessage(), rootCause(failure));
        closeCheckedCopy(exchange);
        exchange.getMessage().setBody(null);
        exchange.setException(failure);
        exchange.setRouteStop(true);
    }

    /**
     * The innermost cause, so that a full disk, a refused or missing object and a timeout can be
     * told apart. Its message is shown only for a failure while the object was copied or opened:
     * an I/O or file system error, or the object store client's own error. Any other cause, such
     * as a parser's or an application's, may quote payload content, so only its class is shown.
     * The text is one line.
     */
    static String rootCause(Throwable failure) {
        Throwable root = failure;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        if (root == failure) {
            return "";
        }
        String message = root.getMessage();
        boolean safe = message != null && !root.getClass().getName().startsWith("com.fasterxml.")
                && (root instanceof java.io.IOException || isObjectStoreClientError(root));
        String text = " (cause: " + root.getClass().getName() + (safe ? ": " + message : "") + ")";
        return text.replaceAll("[\\r\\n\\t]+", " ");
    }

    private static boolean isObjectStoreClientError(Throwable failure) {
        for (Class<?> type = failure.getClass(); type != null; type = type.getSuperclass()) {
            if (type.getName().equals("software.amazon.awssdk.core.exception.SdkException")) {
                return true;
            }
        }
        return false;
    }

    private void closeCheckedCopy(Exchange exchange) {
        InputStream copy = exchange.getProperty(CHECKED_COPY, InputStream.class);
        if (copy == null) {
            return;
        }
        exchange.removeProperty(CHECKED_COPY);
        try {
            copy.close();
        } catch (Exception e) {
            LOG.warn("Could not close the checked payload copy of message {}: {}",
                    exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID), e.toString());
        }
    }

    private InputStream checkedCopy(Exchange exchange, PayloadReference reference) {
        InputStream copy = payloadSpool.checkedCopy(reference, () -> objectStore.open(reference));
        exchange.setProperty(CHECKED_COPY, copy);
        return copy;
    }

    @Override
    public void onExchangeDone(Route route, Exchange exchange) {
        // The route is done with its payload; the checked copy and its file go now.
        closeCheckedCopy(exchange);
        if (boundaryKind == MessageBoundaryKind.KAFKA && parkingDidNotPublish(exchange)) {
            // Neither completed nor parked: keep the exchange failed, so that it is recorded
            // FAILED, not committed, and read again.
            exchange.setException(new IllegalStateException(
                    "The DLQ parking route did not publish; the message was not parked"));
        }
        if (recordingSuppressed(exchange)) {
            LOG.debug("Skipped message-store recording for an ineligible exchange on route {}", route.getRouteId());
            acknowledge(route, exchange);
            return;
        }

        MessageAttemptResult result = result(route, exchange);

        try {
            sink.record(result);
        } catch (Exception e) {
            String consequence = boundaryKind == MessageBoundaryKind.KAFKA
                    ? "Kafka offset was not committed"
                    : "source response was not completed";
            IllegalStateException failure = new IllegalStateException(
                    "Failed to record message outcome; " + consequence, e);

            // Camel logs and otherwise ignores exceptions thrown by
            // RoutePolicy#onExchangeDone. Mark the exchange failed as well so
            // source consumers (notably platform-http) cannot return success.
            exchange.setException(failure);
            if (boundaryKind == MessageBoundaryKind.HTTP) {
                exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 500);
                exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, "text/plain");
                exchange.getMessage().setBody("Message outcome could not be recorded");
            }
            if (boundaryKind == MessageBoundaryKind.KAFKA) {
                backOffBeforeReadingAgain(route, exchange);
            }
            throw failure;
        }
        try {
            acknowledge(route, exchange);
        } catch (Exception e) {
            IllegalStateException failure = new IllegalStateException(
                    "Message outcome was recorded but Kafka offset commit failed", e);
            exchange.setException(failure);
            throw failure;
        }
    }

    private MessageAttemptResult result(Route route, Exchange exchange) {
        boolean dlqParked = dlqParked(exchange);
        Throwable failure = failure(exchange, dlqParked);
        // The input is consumed again, also when a route handled the failure.
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
                payloadRef(exchange),
                payloadSha256(exchange),
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

    /**
     * Commits a Kafka input that completed or whose DLQ parking was confirmed. An input that
     * is still failed was neither, and an input whose attempt could not check or record a step
     * outcome must be consumed again: its offset is not committed, and its exchange stays
     * failed, so the Kafka consumer reads the record again.
     */
    private void acknowledge(Route route, Exchange exchange) {
        Throwable unrecorded = StepOutcomeFailure.of(exchange);
        if (boundaryKind == MessageBoundaryKind.KAFKA && unrecorded != null) {
            LOG.error("Message {} is not committed: a step outcome was not checked or recorded, so it is consumed again",
                    exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID), unrecorded);
            if (exchange.getException() == null) {
                // The route handled the failure; the Kafka consumer reads the record again only from a failed exchange.
                exchange.setException(new IllegalStateException(
                        "A step outcome was not checked or recorded; Kafka offset was not committed", unrecorded));
            }
            backOffBeforeReadingAgain(route, exchange);
            return;
        }
        if (boundaryKind == MessageBoundaryKind.KAFKA && exchange.getException() != null && !dlqParked(exchange)) {
            LOG.error("Message {} failed without confirmed DLQ parking; its Kafka offset was not committed",
                    exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID));
            backOffBeforeReadingAgain(route, exchange);
            return;
        }
        commitKafkaOffsetIfPresent(exchange);
        String position = exchange.getProperty(RECORD_POSITION, String.class);
        if (position != null) {
            heldBack.remove(partition(position));
        }
        if (Boolean.TRUE.equals(exchange.getProperty(LAST_POLL_RECORD, Boolean.class))) {
            endOfPoll(exchange);
        }
    }

    private void commitKafkaOffsetIfPresent(Exchange exchange) {
        if (boundaryKind != MessageBoundaryKind.KAFKA || !kafkaCommitEnabled) {
            return;
        }
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
        if (isPayloadReference(exchange)) {
            resolvePayloadReference(exchange, body);
            return;
        }

        byte[] bytes = payloadBytes(body);
        Long sizeBytes = bytes == null ? payloadSizeBytes(body) : Long.valueOf(bytes.length);
        if (sizeBytes != null) {
            exchange.setProperty(PlatformExchangeProperties.PAYLOAD_SIZE_BYTES, sizeBytes);
        }

        if (!payloadPolicy.replayEnabled()) {
            metadataOnly(exchange, "replay disabled by policy");
            keepForParking(exchange, body);
            return;
        }
        if (payloadPolicy.mode() == PayloadMode.METADATA_ONLY) {
            metadataOnly(exchange, "payload storage mode is metadata-only");
            keepForParking(exchange, body);
            return;
        }
        if (bytes == null && body instanceof InputStream input && payloadPolicy.mode() == PayloadMode.TIERED) {
            captureStream(exchange, input);
            return;
        }
        if (bytes == null) {
            metadataOnly(exchange, "payload body type is not safely capturable");
            return;
        }
        if (bytes.length > payloadPolicy.inlineThresholdBytes() && payloadPolicy.mode() == PayloadMode.TIERED) {
            captureObject(exchange, new ByteArrayInputStream(bytes), (long) bytes.length);
            return;
        }
        if (bytes.length > payloadPolicy.inlineThresholdBytes()) {
            metadataOnly(exchange, "payload exceeds max inline size");
            keepForParking(exchange, body);
            return;
        }

        // A route may change a byte array body in place; replay and parking keep the consumed input.
        byte[] captured = body instanceof byte[] ? bytes.clone() : bytes;
        exchange.setProperty(PlatformExchangeProperties.PAYLOAD_STORAGE, PayloadMode.POSTGRES_INLINE.storageValue());
        exchange.setProperty(PlatformExchangeProperties.PAYLOAD_BYTES, captured);
        exchange.setProperty(PlatformExchangeProperties.PAYLOAD_SHA256, sha256(captured));
        exchange.setProperty(PlatformExchangeProperties.PAYLOAD_EXPIRES_AT,
                Instant.now().plus(payloadPolicy.retention()));
        exchange.setProperty(PlatformExchangeProperties.REPLAYABLE, true);
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
        exchange.removeProperty(PlatformExchangeProperties.PAYLOAD_REF);
        exchange.removeProperty(PlatformExchangeProperties.PAYLOAD_SHA256);
        exchange.removeProperty(PlatformExchangeProperties.PAYLOAD_EXPIRES_AT);
        exchange.setProperty(PlatformExchangeProperties.REPLAYABLE, false);
        exchange.setProperty(PlatformExchangeProperties.REPLAY_DISABLED_REASON, reason);
    }

    private void captureStream(Exchange exchange, InputStream input) {
        try {
            int probeSize = Math.toIntExact(payloadPolicy.inlineThresholdBytes() + 1);
            byte[] probe = input.readNBytes(probeSize);
            if (probe.length <= payloadPolicy.inlineThresholdBytes()) {
                // The route gets its own array, so the capture stays the consumed input.
                exchange.getMessage().setBody(probe.clone());
                exchange.setProperty(PlatformExchangeProperties.PAYLOAD_SIZE_BYTES, (long) probe.length);
                exchange.setProperty(PlatformExchangeProperties.PAYLOAD_STORAGE, PayloadMode.POSTGRES_INLINE.storageValue());
                exchange.setProperty(PlatformExchangeProperties.PAYLOAD_BYTES, probe);
                exchange.setProperty(PlatformExchangeProperties.PAYLOAD_SHA256, sha256(probe));
                exchange.setProperty(PlatformExchangeProperties.PAYLOAD_EXPIRES_AT,
                        Instant.now().plus(payloadPolicy.retention()));
                exchange.setProperty(PlatformExchangeProperties.REPLAYABLE, true);
                return;
            }
            InputStream completePayload = new SequenceInputStream(new ByteArrayInputStream(probe), input);
            PayloadReference reference = captureObject(exchange, completePayload, null);
            exchange.getMessage().setBody(checkedCopy(exchange, reference));
        } catch (PayloadStorageException e) {
            throw e;
        } catch (Exception e) {
            throw new PayloadStorageException("Failed to capture streaming payload", e);
        }
    }

    private PayloadReference captureObject(Exchange exchange, InputStream input, Long declaredSize) {
        Instant expiresAt = Instant.now().plus(payloadPolicy.retention());
        PayloadReference reference = objectStore.store(
                integrationName,
                ensureMessageId(exchange),
                input,
                declaredSize,
                exchange.getMessage().getHeader(PlatformHeaders.CONTENT_TYPE, String.class),
                expiresAt,
                payloadPolicy.objectMaxBytes());
        applyPayloadReference(exchange, reference);
        return reference;
    }

    private void resolvePayloadReference(Exchange exchange, Object body) {
        PayloadReference reference = PayloadReference.fromJson(body);
        validateReferenceHeaders(exchange, reference);
        // Before any check that can fail: a FAILED outcome must keep the message's claim check,
        // not overwrite it with a metadata-only row.
        applyPayloadReference(exchange, reference);
        exchange.setProperty(PlatformExchangeProperties.PAYLOAD_CLAIM_CHECK, reference.toJsonBytes());
        exchange.getMessage().setHeader(PlatformHeaders.CONTENT_TYPE, reference.contentType());
        if (reference.expiryInstant().isBefore(Instant.now())) {
            throw new PayloadStorageException("Payload reference has expired: " + reference.ref());
        }
        // Opening the object checks its existence and size first, so no separate verify call.
        exchange.getMessage().setBody(checkedCopy(exchange, reference));
    }

    private void applyPayloadReference(Exchange exchange, PayloadReference reference) {
        byte[] claimCheck = reference.toJsonBytes();
        exchange.setProperty(PlatformExchangeProperties.PAYLOAD_STORAGE, PayloadReference.STORAGE);
        exchange.setProperty(PlatformExchangeProperties.PAYLOAD_SIZE_BYTES, reference.sizeBytes());
        exchange.removeProperty(PlatformExchangeProperties.PAYLOAD_BYTES);
        exchange.setProperty(PlatformExchangeProperties.PAYLOAD_REF, reference.ref());
        exchange.setProperty(PlatformExchangeProperties.PAYLOAD_SHA256, reference.sha256());
        exchange.setProperty(PlatformExchangeProperties.PAYLOAD_CLAIM_CHECK, claimCheck);
        exchange.setProperty(PlatformExchangeProperties.PAYLOAD_EXPIRES_AT, reference.expiryInstant());
        exchange.setProperty(PlatformExchangeProperties.REPLAYABLE, true);
        exchange.removeProperty(PlatformExchangeProperties.REPLAY_DISABLED_REASON);
        exchange.getMessage().setHeader(PlatformHeaders.PAYLOAD_STORAGE, PayloadReference.STORAGE);
        exchange.getMessage().setHeader(PlatformHeaders.PAYLOAD_REF, reference.ref());
        exchange.getMessage().setHeader(PlatformHeaders.PAYLOAD_SIZE, Long.toString(reference.sizeBytes()));
        exchange.getMessage().setHeader(PlatformHeaders.PAYLOAD_SHA256, reference.sha256());
    }

    private boolean isPayloadReference(Exchange exchange) {
        String storage = exchange.getMessage().getHeader(PlatformHeaders.PAYLOAD_STORAGE, String.class);
        String contentType = exchange.getMessage().getHeader(PlatformHeaders.TRANSPORT_CONTENT_TYPE, String.class);
        return PayloadReference.STORAGE.equals(storage) || PayloadReference.CONTENT_TYPE.equals(contentType);
    }

    private void validateReferenceHeaders(Exchange exchange, PayloadReference reference) {
        matchesHeader(exchange, PlatformHeaders.PAYLOAD_STORAGE, reference.storage());
        matchesHeader(exchange, PlatformHeaders.PAYLOAD_REF, reference.ref());
        matchesHeader(exchange, PlatformHeaders.PAYLOAD_SIZE, Long.toString(reference.sizeBytes()));
        matchesHeader(exchange, PlatformHeaders.PAYLOAD_SHA256, reference.sha256());
    }

    private void matchesHeader(Exchange exchange, String name, String expected) {
        String actual = exchange.getMessage().getHeader(name, String.class);
        if (actual != null && !actual.equals(expected)) {
            throw new PayloadStorageException("Payload-reference header " + name + " does not match its document");
        }
    }

    private String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (Exception e) {
            throw new PayloadStorageException("Failed to calculate payload checksum", e);
        }
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
    // (a grouped split, removeHeaders). The outcome still belongs to the consumed input, and a
    // replay stays its own attempt instead of overwriting attempt 1's history row.
    private String messageId(Exchange exchange) {
        String current = exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID, String.class);
        String input = exchange.getProperty(PlatformExchangeProperties.INPUT_MESSAGE_ID, String.class);
        if ((current == null || current.isBlank()) && input != null) {
            exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, input);
        }
        return ensureMessageId(exchange);
    }

    private String correlationId(Exchange exchange) {
        String current = exchange.getMessage().getHeader(PlatformHeaders.CORRELATION_ID, String.class);
        return current != null ? current : exchange.getProperty(INPUT_CORRELATION_ID, String.class);
    }

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

    // Kafka exposes header values as bytes; receivers and the journal need the owned text.
    // The x-bip-* headers are UTF-8 text, independent of the payload charset.
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

    private String payloadRef(Exchange exchange) {
        return exchange.getProperty(PlatformExchangeProperties.PAYLOAD_REF, String.class);
    }

    private String payloadSha256(Exchange exchange) {
        return exchange.getProperty(PlatformExchangeProperties.PAYLOAD_SHA256, String.class);
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
        Throwable caught = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
        if (includeCaughtException || exchange.isFailed() || boundaryKind != MessageBoundaryKind.KAFKA) {
            if (caught != null) {
                return caught;
            }
        }
        if (boundaryKind == MessageBoundaryKind.HTTP) {
            Integer responseCode = exchange.getMessage().getHeader(Exchange.HTTP_RESPONSE_CODE, Integer.class);
            if (responseCode != null && responseCode >= 400) {
                return new IllegalStateException("HTTP response status " + responseCode);
            }
        }
        return null;
    }

    private boolean dlqParked(Exchange exchange) {
        return Boolean.TRUE.equals(exchange.getProperty(PlatformExchangeProperties.DLQ_PARKED, Boolean.class));
    }

    /**
     * Whether the dead-letter channel handed the exchange to the adapter's dead-letter route,
     * which did not publish it. Camel then treats the failure as handled. It happens to an
     * exchange that its route stopped before it failed: Camel skips both the adapter's
     * preparation and the dead-letter route for a stopped exchange.
     */
    private static boolean parkingDidNotPublish(Exchange exchange) {
        String sentTo = exchange.getProperty(ExchangePropertyKey.TO_ENDPOINT, String.class);
        return sentTo != null && exchange.getException() == null
                && !Boolean.TRUE.equals(exchange.getProperty(PlatformExchangeProperties.DLQ_PARKED, Boolean.class))
                && EndpointHelper.matchEndpoint(exchange.getContext(), sentTo, "direct:" + PlatformDeadLetterRoute.ROUTE_ID);
    }

    private boolean recordingSuppressed(Exchange exchange) {
        Boolean recordMessage = exchange.getProperty(PlatformExchangeProperties.RECORD_MESSAGE, Boolean.class);
        return Boolean.FALSE.equals(recordMessage);
    }

    private String errorMessage(Throwable failure) {
        if (failure == null) {
            return null;
        }
        return failure.getMessage() == null ? failure.getClass().getName() : failure.getMessage();
    }
}
