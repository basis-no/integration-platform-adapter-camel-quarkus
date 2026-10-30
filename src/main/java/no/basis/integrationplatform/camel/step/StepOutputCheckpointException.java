package no.basis.integrationplatform.camel.step;

/** Signals that a required step output cannot be captured or restored safely. */
public class StepOutputCheckpointException extends RuntimeException {
    /**
     * Creates an output-checkpoint failure.
     *
     * @param message failure description
     */
    public StepOutputCheckpointException(String message) {
        super(message);
    }

    /**
     * Creates an output-checkpoint failure caused by another exception.
     *
     * @param message failure description
     * @param cause underlying failure
     */
    public StepOutputCheckpointException(String message, Throwable cause) {
        super(message, cause);
    }
}
