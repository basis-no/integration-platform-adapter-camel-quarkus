package no.basis.integrationplatform.camel.model;

/** Terminal status vocabulary for one classified integration step. */
public enum StepStatus {
    /** The external call completed and its outcome was stored durably. */
    DELIVERED,
    /** The external call failed. */
    FAILED,
    /** The call was suppressed because its durable outcome already existed. */
    SKIPPED
}
