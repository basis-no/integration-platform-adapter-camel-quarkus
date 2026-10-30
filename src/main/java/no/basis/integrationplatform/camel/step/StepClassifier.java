package no.basis.integrationplatform.camel.step;

/**
 * Classifies an endpoint send by replay semantics. Applications may provide an
 * {@code @ApplicationScoped} implementation to replace the adapter default.
 */
public interface StepClassifier {
    /**
     * Returns the replay role for one intercepted endpoint send.
     *
     * @param context intercepted endpoint and exchange context
     * @return replay role governing execution and durable tracking
     */
    StepRole classify(StepClassificationContext context);
}
