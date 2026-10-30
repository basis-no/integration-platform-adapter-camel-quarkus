package no.basis.integrationplatform.camel.ingress;

/** Indicates that a source exchange cannot be normalized safely into Kafka. */
public class IngressNormalizationException extends RuntimeException {
    /**
     * Creates a normalization failure.
     *
     * @param message actionable failure description
     */
    public IngressNormalizationException(String message) {
        super(message);
    }
}
