package no.basis.integrationplatform.camel.payload;

/**
 * A payload reference that can never resolve to the bytes it describes: the object store
 * answered that its object does not exist, the object does not match the reference's size or
 * SHA-256, or the reference has expired or is malformed. Reading the input again cannot heal
 * it, so a Kafka input that arrives as such a claim check is parked in the DLQ instead of
 * being read again.
 * <p>
 * An application's own {@link PayloadObjectStore} throws it only when the store itself has
 * answered that the object does not exist. A failure that may pass, such as an unreachable
 * store, a refused access, a missing bucket or a timeout, is a plain
 * {@link PayloadStorageException}.
 */
public class UnresolvablePayloadException extends PayloadStorageException {
    /**
     * Creates the failure.
     *
     * @param message why the reference can never resolve
     */
    public UnresolvablePayloadException(String message) {
        super(message);
    }

    /**
     * Creates the failure with the object store's answer.
     *
     * @param message why the reference can never resolve
     * @param cause the object store's answer
     */
    public UnresolvablePayloadException(String message, Throwable cause) {
        super(message, cause);
    }
}
