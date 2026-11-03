package no.basis.integrationplatform.camel.model;

public enum MessageStatus {
    /**
     * Reserved for a future capture layer: the message has entered durable custody.
     * The Camel adapter does not emit this as a terminal route outcome.
     */
    RECEIVED,

    /**
     * Terminal adapter outcome: route processing completed and the outcome was safely recorded.
     */
    COMPLETED,

    /**
     * Terminal adapter outcome for a failed exchange that was not successfully parked as DLQ.
     * Normal Camel retry/redelivery noise is not emitted as separate FAILED outcomes.
     */
    FAILED,

    /**
     * Terminal adapter outcome: retries were exhausted and the message was parked for operator action.
     */
    DLQ
}
