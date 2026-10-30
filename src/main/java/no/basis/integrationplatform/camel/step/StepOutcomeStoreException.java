package no.basis.integrationplatform.camel.step;

/** Signals that a durable step outcome could not be read or written. */
public class StepOutcomeStoreException extends RuntimeException {
    /**
     * Creates a step-outcome storage failure.
     *
     * @param message failure description
     * @param cause underlying storage failure
     */
    public StepOutcomeStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
