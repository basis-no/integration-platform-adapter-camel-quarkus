package no.basis.integrationplatform.camel.lifecycle;

import no.basis.integrationplatform.camel.KafkaPollInterval;
import no.basis.integrationplatform.camel.KafkaRecordHeaders;
import org.apache.camel.ProducerTemplate;
import no.basis.integrationplatform.camel.step.UnrecordedDeliveries;
import no.basis.integrationplatform.camel.StoreErrorClass;
import no.basis.integrationplatform.camel.PlatformDeadLetterRoute;
import no.basis.integrationplatform.camel.PlatformExchangeProperties;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.payload.PayloadObjectStore;
import no.basis.integrationplatform.camel.payload.PayloadReference;
import no.basis.integrationplatform.camel.payload.PayloadStorageException;
import no.basis.integrationplatform.camel.payload.UnresolvablePayloadException;
import no.basis.integrationplatform.camel.step.DefaultStepTracker;
import no.basis.integrationplatform.camel.step.StepOutcomeFailure;
import no.basis.integrationplatform.camel.store.MessageStoreSink;
import no.basis.integrationplatform.camel.traffic.TrafficSignals;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePropertyKey;
import org.apache.camel.ProducerTemplate;
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
    private static final String HTTP_NOT_REPLAYABLE = "a synchronous HTTP boundary is not replayable";
    private static final String MALFORMED_CLAIM_CHECK = "the claim check is malformed or does not match its headers";
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
    private volatile ProducerTemplate deadLetter;
    // Per topic partition: the offset of the record held back last, and how often in a row.
    private final Map<String, long[]> heldBack = new ConcurrentHashMap<>();
    private Sleeper sleeper = MessageStoreRoutePolicy::sleepUnlessStopping;

    static final long FIRST_BACK_OFF_MS = 1_000;
    static final long MAX_BACK_OFF_MS = 30_000;
    private static final int MAX_HELD_BACK_PARTITIONS = 1_024;
    private static final String POLL_STARTED = "basis.messageStore.pollStarted";
    private static final String LAST_POLL_RECORD = "basis.messageStore.lastPollRecord";
    // Per consumer thread: when its current poll's records began, and whether the next record
    // starts a new poll. A failed record ends its poll: with breakOnFirstError the consumer
    // stops the batch and reconnects.
    private final Map<Long, Long> pollStarted = new ConcurrentHashMap<>();
    private final Set<Long> newPollNext = ConcurrentHashMap.newKeySet();
    // Per topic partition: the offset of a held-back record that came without x-bip-message-id,
    // and the id generated for it, so that its next read keeps the id and its marks match.
    private final Map<String, String[]> generatedIds = new ConcurrentHashMap<>();
    // Per topic partition: how many records in a row took the terminal path because the store
    // refused their data. Records the boundary check refused do not count: they say nothing
    // about the store's settings or schema.
    private final Map<String, Integer> recordDataInARow = new ConcurrentHashMap<>();
    // Records parked for their own data in this process, so that one whose reduced row cannot
    // be written is not parked again on every read.
    private final Set<String> parkedForData = java.util.Collections.newSetFromMap(
            java.util.Collections.synchronizedMap(new java.util.LinkedHashMap<>(16, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                    return size() > MAX_HELD_BACK_PARTITIONS;
                }
            }));
    private volatile ProducerTemplate parking;

    /** Above this many bytes, a stored text envelope value is refused at the boundary. */
    static final int MAX_ENVELOPE_VALUE_BYTES = 1_024;
    /** After this many records in a row parked for their own data, the next one blocks instead. */
    static final int MAX_RECORD_DATA_IN_A_ROW = 3;
    private static final String ENVELOPE_REFUSAL = "basis.messageStore.envelopeRefusal";
    // The record headers the Message Store keeps as text, two of them in indexes.
    private static final List<String> STORED_TEXT_HEADERS = List.of(PlatformHeaders.MESSAGE_ID,
            PlatformHeaders.CORRELATION_ID, PlatformHeaders.CONTENT_TYPE, PlatformHeaders.SOURCE_SYSTEM,
            PlatformHeaders.TARGET_SYSTEM);

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
        if (boundaryKind == MessageBoundaryKind.KAFKA) {
            captureRecordPosition(exchange);
            // On the record's own bytes, before an id is assigned or the payload captured.
            refuseUnstorableEnvelope(exchange);
        }
        normalizeKafkaEnvelopeHeaders(exchange);
        // A replay's redo list is for this attempt only; the route must not forward it.
        DefaultStepTracker.captureRedoSteps(exchange);
        exchange.setProperty(PlatformExchangeProperties.INPUT_MESSAGE_ID, boundaryKind == MessageBoundaryKind.KAFKA
                ? keepGeneratedId(exchange) : ensureMessageId(exchange));
        exchange.setProperty(INPUT_CORRELATION_ID, exchange.getMessage().getHeader(PlatformHeaders.CORRELATION_ID, String.class));
        captureInputEnvelope(exchange);
        if (boundaryKind == MessageBoundaryKind.KAFKA) {
            captureKafkaManualCommit(exchange);
            capturePollStart(exchange);
            // A later record on the partition supersedes the marks of the one before it.
            UnrecordedDeliveries.supersede(exchange, false);
        }
        if (exchange.getProperty(PlatformExchangeProperties.KAFKA_MANUAL_COMMIT) != null) {
            // Only an input the adapter parks and commits is held back by its attempt's step outcomes.
            StepOutcomeFailure.begin(exchange);
        }
        // Before the capture: a parking route may change the record's headers.
        if (trafficSignals != null && boundaryKind == MessageBoundaryKind.KAFKA) {
            trafficSignals.recordAdmitted(exchange);
        }
        // Before the capture, which makes a streamed input the adapter stores a claim check too.
        boolean claimCheck = isPayloadReference(exchange);
        try {
            capturePayloadMetadata(exchange);
        } catch (PayloadStorageException e) {
            failBeforeTheRoute(exchange, e, claimCheck);
        } catch (RuntimeException e) {
            // An application's own PayloadObjectStore may throw anything.
            failBeforeTheRoute(exchange, new PayloadStorageException(
                    "The payload could not be captured or checked: " + e.getClass().getSimpleName(), e), claimCheck);
        }
        if (exchange.getProperty(ENVELOPE_REFUSAL) != null) {
            // The route does not run; onExchangeDone parks and records the record.
            exchange.setRouteStop(true);
        }
    }

    /**
     * Refuses a record whose text envelope values the Message Store cannot hold: a value with a
     * NUL byte, or longer than {@value #MAX_ENVELOPE_VALUE_BYTES} bytes. When the message id
     * itself is refused, the record gets the id derived from its position.
     */
    private void refuseUnstorableEnvelope(Exchange exchange) {
        List<String> refused = new java.util.ArrayList<>();
        boolean idRefused = false;
        for (String name : STORED_TEXT_HEADERS) {
            Object value = exchange.getMessage().getHeader(name);
            byte[] bytes = value instanceof byte[] raw ? raw
                    : value == null ? null : value.toString().getBytes(StandardCharsets.UTF_8);
            String why = bytes == null ? null : containsNul(bytes) ? "contains a NUL byte"
                    : bytes.length > MAX_ENVELOPE_VALUE_BYTES
                    ? "is " + bytes.length + " bytes, over the " + MAX_ENVELOPE_VALUE_BYTES + "-byte limit" : null;
            if (why != null) {
                refused.add(name + " " + why);
                idRefused |= name.equals(PlatformHeaders.MESSAGE_ID);
            }
        }
        if (refused.isEmpty()) {
            return;
        }
        if (idRefused) {
            String derived = derivedMessageId(exchange);
            if (derived == null) {
                exchange.getMessage().removeHeader(PlatformHeaders.MESSAGE_ID);
            } else {
                exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, derived);
            }
        }
        exchange.setProperty(ENVELOPE_REFUSAL, "The Message Store cannot hold the record's " + String.join(", ", refused)
                + "; the route did not run");
    }

    private static boolean containsNul(byte[] bytes) {
        for (byte b : bytes) {
            if (b == 0) {
                return true;
            }
        }
        return false;
    }

    private String derivedMessageId(Exchange exchange) {
        String position = exchange.getProperty(PlatformExchangeProperties.RECORD_POSITION, String.class);
        if (position == null) {
            return null;
        }
        int slash = position.lastIndexOf('/');
        int at = position.lastIndexOf('@');
        return KafkaRecordIdentity.derive(integrationName, position.substring(0, slash),
                Integer.parseInt(position.substring(slash + 1, at)), Long.parseLong(position.substring(at + 1)));
    }

    /**
     * The record's message id. A record without one gets a generated id, and keeps it when the
     * same record is read again in this process while it is held back, so that the marks of its
     * sends match.
     */
    private String keepGeneratedId(Exchange exchange) {
        String current = exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID, String.class);
        String position = exchange.getProperty(PlatformExchangeProperties.RECORD_POSITION, String.class);
        if (position != null && (current != null && !current.isBlank())) {
            String[] kept = generatedIds.get(partition(position));
            if (kept != null && !kept[0].equals(position.substring(position.lastIndexOf('@') + 1))) {
                generatedIds.remove(partition(position));
            }
        }
        if ((current != null && !current.isBlank()) || position == null) {
            return ensureMessageId(exchange);
        }
        String offset = position.substring(position.lastIndexOf('@') + 1);
        String[] kept = generatedIds.get(partition(position));
        if (kept != null && !kept[0].equals(offset)) {
            // Another record began on the partition, which supersedes the kept id.
            generatedIds.remove(partition(position));
            kept = null;
        }
        if (kept != null) {
            exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, kept[1]);
            return kept[1];
        }
        String generated = ensureMessageId(exchange);
        if (generatedIds.size() >= MAX_HELD_BACK_PARTITIONS && !generatedIds.containsKey(partition(position))) {
            generatedIds.keySet().stream().findFirst().ifPresent(generatedIds::remove);
        }
        generatedIds.put(partition(position), new String[]{offset, generated});
        return generated;
    }

    /**
     * Camel ignores an exception thrown from {@code onExchangeBegin}, and runs the route. A
     * payload that could not be captured or checked must not reach it: the exchange stops, so
     * no processor runs. A failure that may pass fails the exchange, so its outcome is recorded
     * FAILED without a commit and the record is read again. A Kafka input that arrived as a
     * claim check that can never resolve is parked instead, and committed once the DLQ has
     * accepted it.
     */
    private void failBeforeTheRoute(Exchange exchange, PayloadStorageException failure, boolean claimCheck) {
        // Not the first attempt of a streamed input: its record is the source, and is stored again.
        boolean park = claimCheck && failure instanceof UnresolvablePayloadException
                && boundaryKind == MessageBoundaryKind.KAFKA
                && exchange.getProperty(PlatformExchangeProperties.KAFKA_MANUAL_COMMIT) != null;
        if (park && exchange.getProperty(PlatformExchangeProperties.PAYLOAD_CLAIM_CHECK) == null) {
            // A claim check that could not be read is parked as it came, if it can be kept.
            Object body = exchange.getMessage().getBody();
            keepForParking(exchange, body);
            park = exchange.getProperty(PlatformExchangeProperties.PARKING_PAYLOAD) != null;
            if (park) {
                metadataOnly(exchange, MALFORMED_CLAIM_CHECK);
                exchange.setProperty(PlatformExchangeProperties.PAYLOAD_SIZE_BYTES, payloadSizeBytes(body));
            }
        }
        // One line: the message id and a reference's ref come from the producer.
        LOG.error("{}", oneLine("Message " + exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID)
                + " did not reach its route: " + failure.getMessage() + rootCause(failure)
                + (park ? "; it can never pass its check, so it is parked in the DLQ" : "")));
        closeCheckedCopy(exchange);
        exchange.getMessage().setBody(null);
        if (park) {
            parkBeforeTheRoute(exchange, failure);
        } else {
            exchange.setException(failure);
        }
        exchange.setRouteStop(true);
    }

    /**
     * Parks the input through the adapter's dead-letter route, as the dead letter channel does,
     * but without the route's error handler: a redelivery would run the route without its
     * payload. A publish that fails leaves the exchange failed and not parked, so the record is
     * read again.
     */
    private void parkBeforeTheRoute(Exchange exchange, PayloadStorageException failure) {
        exchange.setProperty(Exchange.EXCEPTION_CAUGHT, failure);
        try {
            deadLetter(exchange).send("direct:" + PlatformDeadLetterRoute.ROUTE_ID, exchange);
        } catch (RuntimeException e) {
            exchange.setException(e);
        }
        if (!dlqParked(exchange)) {
            Throwable publish = exchange.getException();
            LOG.error("{}", oneLine("Message " + exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID)
                    + " could not be parked, so it is read again: "
                    + (publish == null ? "the DLQ route did not publish it" : publish.toString())));
            exchange.setException(failure);
        }
    }

    private ProducerTemplate deadLetter(Exchange exchange) {
        ProducerTemplate current = deadLetter;
        if (current == null) {
            synchronized (this) {
                current = deadLetter;
                if (current == null) {
                    current = exchange.getContext().createProducerTemplate();
                    deadLetter = current;
                }
            }
        }
        return current;
    }

    @Override
    protected void doStop() throws Exception {
        ProducerTemplate current = deadLetter;
        deadLetter = null;
        if (current != null) {
            current.stop();
        }
        super.doStop();
    }

    /**
     * The innermost cause, so that a full disk, a refused or missing object and a timeout can be
     * told apart. Its message is shown only for a failure while the object was copied or opened:
     * an I/O or file system error, or the object store client's own error. Any other cause, such
     * as a parser's or an application's, may quote payload content, so only its class is shown.
     * The text is one line. This covers the adapter's own log line only: the failure stays the
     * exchange's exception, with its causes, for Camel and the sink to report.
     */
    static String rootCause(Throwable failure) {
        Throwable root = failure;
        java.util.Set<Throwable> seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        while (root.getCause() != null && seen.add(root)) {
            root = root.getCause();
        }
        if (root == failure) {
            return "";
        }
        String message = root.getMessage();
        boolean safe = message != null && !root.getClass().getName().startsWith("com.fasterxml.")
                && (root instanceof java.io.IOException || isObjectStoreClientError(root));
        return oneLine(" (cause: " + root.getClass().getName() + (safe ? ": " + message : "") + ")");
    }

    /** The text with every control character and line separator replaced, so that it is one log line. */
    static String oneLine(String text) {
        return text.replaceAll("[\\p{Cc}\\u2028\\u2029]+", " ");
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
            LOG.warn("{}", oneLine("Could not close the checked payload copy of message "
                    + exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID) + ": " + e));
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
        if (boundaryKind == MessageBoundaryKind.KAFKA) {
            String refusal = exchange.getProperty(ENVELOPE_REFUSAL, String.class);
            Throwable unrecorded = StepOutcomeFailure.of(exchange);
            if (refusal != null) {
                parkForItsOwnData(route, exchange, refusal, false, false);
                return;
            }
            // Only when every step outcome failure was the record's data: a part that failed for
            // another reason has an outcome that must not be dropped by a commit.
            if (StepOutcomeFailure.onlyRecordData(exchange)) {
                parkForItsOwnData(route, exchange, stepDataReason(exchange, unrecorded), false, true);
                return;
            }
        }

        MessageAttemptResult result = result(route, exchange);

        try {
            sink.record(result);
        } catch (Exception e) {
            // Only when every step outcome is durable: a step outcome the store could not take
            // otherwise means the record must be read again, whatever the outcome write says.
            if (boundaryKind == MessageBoundaryKind.KAFKA && StoreErrorClass.of(e) == StoreErrorClass.RECORD_DATA
                    && StepOutcomeFailure.of(exchange) == null) {
                parkForItsOwnData(route, exchange, "The Message Store cannot hold the record's outcome (SQLSTATE "
                        + StoreErrorClass.sqlState(e) + ")", result.status() == MessageStatus.COMPLETED, true);
                return;
            }
            if (boundaryKind == MessageBoundaryKind.KAFKA) {
                LOG.error("Message {} is not committed: its outcome could not be recorded, so it is consumed again{}",
                        exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID), StoreErrorClass.logSuffix(e), e);
            }
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
            LOG.error("Message {} is not committed: a step outcome was not checked or recorded, so it is consumed again{}",
                    exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID), StoreErrorClass.logSuffix(unrecorded),
                    unrecorded);
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
        committed(exchange);
        String position = exchange.getProperty(PlatformExchangeProperties.RECORD_POSITION, String.class);
        if (position != null) {
            recordDataInARow.remove(partition(position));
        }
    }

    /** What a committed record ends: its partition's wait, kept id and marks. */
    private void committed(Exchange exchange) {
        String position = exchange.getProperty(PlatformExchangeProperties.RECORD_POSITION, String.class);
        if (position != null) {
            heldBack.remove(partition(position));
            generatedIds.remove(partition(position));
            UnrecordedDeliveries.supersede(exchange, true);
        }
        if (Boolean.TRUE.equals(exchange.getProperty(LAST_POLL_RECORD, Boolean.class))) {
            endOfPoll(exchange);
        }
    }

    private static String stepDataReason(Exchange exchange, Throwable unrecorded) {
        List<String> sent = UnrecordedDeliveries.markedSteps(exchange);
        return "The Message Store cannot hold a step outcome of the record (SQLSTATE " + StoreErrorClass.sqlState(unrecorded)
                + "); " + (sent.isEmpty() ? "no send of the attempt had returned without a recorded outcome"
                : "the send of step " + String.join(", ", sent) + " had returned, its outcome not recorded");
    }

    /**
     * The terminal path for a record whose own data the Message Store cannot hold: park it,
     * unless it was parked already or its route completed, record a reduced outcome without the
     * envelope's text values and steps, and commit only after both are durable. If either fails,
     * the record is read again and the path runs again. After {@value #MAX_RECORD_DATA_IN_A_ROW}
     * such records in a row on a partition, the next one is held back instead, since a setting or
     * column that fails every record must not drain the topic into the DLQ.
     */
    private void parkForItsOwnData(Route route, Exchange exchange, String reason, boolean completed,
            boolean refusedByStore) {
        String position = exchange.getProperty(PlatformExchangeProperties.RECORD_POSITION, String.class);
        String messageId = exchange.getProperty(PlatformExchangeProperties.INPUT_MESSAGE_ID, String.class);
        String parkedKey = route.getRouteId() + "\n" + position;
        if (refusedByStore && position != null
                && recordDataInARow.getOrDefault(partition(position), 0) >= MAX_RECORD_DATA_IN_A_ROW) {
            LOG.error("Message {} at {} is not committed: {} records in a row on this partition could not be stored for "
                            + "their own data, so this one is held back; check the Message Store's settings and schema. "
                            + "{} storeError=schema", messageId, position, MAX_RECORD_DATA_IN_A_ROW, reason);
            readAgain(route, exchange, new IllegalStateException(reason + "; held back after "
                    + MAX_RECORD_DATA_IN_A_ROW + " such records in a row"));
            return;
        }
        if (!completed && !dlqParked(exchange) && !parkedForData.contains(parkedKey) && !park(route, exchange, reason)) {
            LOG.error("Message {} at {} is not committed: it could not be parked for its own data; {} storeError=record-data",
                    messageId, position, reason);
            readAgain(route, exchange, new IllegalStateException("Not parked: " + reason));
            return;
        }
        MessageAttemptResult full = result(route, exchange);
        MessageAttemptResult reduced = new MessageAttemptResult(full.schemaVersion(), messageId, null, null, null, null,
                full.payloadStorage(), full.payloadSizeBytes(), full.payloadBytes(), full.payloadRef(),
                full.payloadSha256(), full.payloadExpiresAt(), full.payloadDeletedAt(), full.replayable(),
                full.replayDisabledReason(), full.integration(), full.routeId(), full.attempt(), full.autoRedelivered(),
                full.autoRedeliveryCount(), full.autoRedeliveryMax(), full.occurredAt(),
                completed ? MessageStatus.COMPLETED : MessageStatus.DLQ, List.of(), reason);
        if (!completed) {
            parkedForData.add(parkedKey);
        }
        try {
            recordReduced(reduced);
        } catch (Exception e) {
            LOG.error("Message {} at {} is not committed: its reduced outcome could not be recorded{}", messageId,
                    position, StoreErrorClass.logSuffix(e), e);
            readAgain(route, exchange, new IllegalStateException("Failed to record the reduced outcome", e));
            return;
        }
        parkedForData.remove(parkedKey);
        try {
            commitKafkaOffsetIfPresent(exchange);
        } catch (Exception e) {
            IllegalStateException failure = new IllegalStateException(
                    "Message outcome was recorded but Kafka offset commit failed", e);
            exchange.setException(failure);
            throw failure;
        }
        committed(exchange);
        if (position != null && refusedByStore) {
            recordDataInARow.merge(partition(position), 1, Integer::sum);
        }
        // Parked and recorded: the consumer goes on with the next record.
        exchange.setException(null);
        LOG.error("Message {} from record {} was {}: {} storeError=record-data", messageId, position,
                completed ? "recorded without its envelope values" : "parked", reason);
    }

    /**
     * Writes the reduced outcome. If the store refuses its data too, for example a payload
     * reference from the record, it is written once more without the payload.
     */
    private void recordReduced(MessageAttemptResult reduced) throws Exception {
        try {
            sink.record(reduced);
        } catch (Exception e) {
            if (StoreErrorClass.of(e) != StoreErrorClass.RECORD_DATA) {
                throw e;
            }
            sink.record(new MessageAttemptResult(reduced.schemaVersion(), reduced.messageId(), null, null, null, null,
                    PayloadMode.METADATA_ONLY.storageValue(), reduced.payloadSizeBytes(), null, null, null, null, null,
                    false, "the record's envelope values could not be stored", reduced.integration(), reduced.routeId(),
                    reduced.attempt(), reduced.autoRedelivered(), reduced.autoRedeliveryCount(),
                    reduced.autoRedeliveryMax(), reduced.occurredAt(), reduced.status(), List.of(), reduced.error()));
        }
    }

    private void readAgain(Route route, Exchange exchange, IllegalStateException failure) {
        exchange.setException(failure);
        backOffBeforeReadingAgain(route, exchange);
        throw failure;
    }

    /**
     * Publishes a copy of the record through the adapter's dead-letter route, which parks the
     * consumed payload, masks credential headers and confirms the publish. The copy carries the
     * reason as its failure and no step outcome failure, which would stop the parking.
     */
    private boolean park(Route route, Exchange exchange, String reason) {
        Exchange copy = exchange.copy();
        copy.setException(null);
        copy.setRouteStop(false);
        copy.removeProperty(PlatformExchangeProperties.DLQ_PARKED);
        copy.setProperty(Exchange.EXCEPTION_CAUGHT, new IllegalStateException(reason));
        StepOutcomeFailure.begin(copy);
        try {
            producer(route).send("direct:" + PlatformDeadLetterRoute.ROUTE_ID, copy);
        } catch (RuntimeException e) {
            return false;
        }
        boolean parked = copy.getException() == null
                && Boolean.TRUE.equals(copy.getProperty(PlatformExchangeProperties.DLQ_PARKED, Boolean.class));
        if (parked) {
            exchange.setProperty(PlatformExchangeProperties.DLQ_PARKED, true);
        }
        return parked;
    }

    private ProducerTemplate producer(Route route) {
        ProducerTemplate current = parking;
        if (current == null) {
            synchronized (this) {
                current = parking;
                if (current == null) {
                    current = route.getCamelContext().createProducerTemplate();
                    parking = current;
                }
            }
        }
        return current;
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
        String position = exchange.getProperty(PlatformExchangeProperties.RECORD_POSITION, String.class);
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
        exchange.setProperty(PlatformExchangeProperties.POLL_THREAD, thread);
        Object last = exchange.getMessage().getHeader("CamelKafkaLastPollRecord");
        if (last == null) {
            last = exchange.getMessage().getHeader("kafka.LAST_POLL_RECORD");
        }
        exchange.setProperty(LAST_POLL_RECORD, Boolean.TRUE.equals(last) || "true".equals(String.valueOf(last)));
    }

    private void endOfPoll(Exchange exchange) {
        Long thread = exchange.getProperty(PlatformExchangeProperties.POLL_THREAD, Long.class);
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
        return route.getEndpoint() instanceof org.apache.camel.component.kafka.KafkaEndpoint kafka
                ? KafkaPollInterval.of(kafka.getConfiguration())
                : KafkaPollInterval.DEFAULT_MS;
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
            exchange.setProperty(PlatformExchangeProperties.RECORD_POSITION, name + "/" + p.intValue() + "@" + o.longValue());
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
            try {
                resolvePayloadReference(exchange, body);
            } finally {
                if (boundaryKind == MessageBoundaryKind.HTTP) {
                    // The route still reads the referenced object, but the row keeps no reference,
                    // also when the object fails its check: nothing could replay it, and the
                    // object belongs to the caller.
                    metadataOnly(exchange, HTTP_NOT_REPLAYABLE);
                }
            }
            return;
        }

        byte[] bytes = payloadBytes(body);
        Long sizeBytes = bytes == null ? payloadSizeBytes(body) : Long.valueOf(bytes.length);
        if (sizeBytes != null) {
            exchange.setProperty(PlatformExchangeProperties.PAYLOAD_SIZE_BYTES, sizeBytes);
        }

        if (boundaryKind == MessageBoundaryKind.HTTP) {
            // A replay cannot answer the original caller, so a synchronous HTTP boundary is
            // never replayable, also when basis.message-store.replay.enabled is true.
            metadataOnly(exchange, HTTP_NOT_REPLAYABLE);
            keepForParking(exchange, body);
            return;
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
        PayloadReference reference;
        try {
            reference = PayloadReference.fromJson(body);
        } catch (PayloadStorageException e) {
            // Its message names only the parser's exception class, never the body.
            throw new UnresolvablePayloadException(e.getMessage());
        }
        validateReferenceHeaders(exchange, reference);
        // Before any check that can fail: a FAILED outcome must keep the message's claim check,
        // not overwrite it with a metadata-only row.
        applyPayloadReference(exchange, reference);
        exchange.setProperty(PlatformExchangeProperties.PAYLOAD_CLAIM_CHECK, reference.toJsonBytes());
        exchange.getMessage().setHeader(PlatformHeaders.CONTENT_TYPE, reference.contentType());
        if (reference.expiryInstant().isBefore(Instant.now())) {
            throw new UnresolvablePayloadException("Payload reference has expired: " + reference.ref());
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
            throw new UnresolvablePayloadException("Payload-reference header " + name + " does not match its document");
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
        String message = failure.getMessage() == null ? failure.getClass().getName() : failure.getMessage();
        // PostgreSQL text cannot hold a NUL; a failure's own text must not make its record unstorable.
        return message.replace('\u0000', '\uFFFD');
    }
}
