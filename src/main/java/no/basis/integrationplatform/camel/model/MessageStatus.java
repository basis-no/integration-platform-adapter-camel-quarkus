package no.basis.integrationplatform.camel.model;

/** Message-store lifecycle status vocabulary shared with the runtime contract. */
public enum MessageStatus {
    /**
     * Reserved for platform capture: the message has entered durable platform custody.
     * The Camel adapter does not emit this as a terminal route outcome in V1.
     */
    RECEIVED,

    /**
     * Terminal adapter outcome: route processing completed and the outcome was safely recorded.
     */
    COMPLETED,

    /**
     * Terminal adapter outcome for a failed exchange that was not successfully parked as DLQ.
     * Normal Camel retry/redelivery noise is not emitted as separate FAILED outcomes in V1.
     */
    FAILED,

    /**
     * Terminal adapter outcome: retries were exhausted and the message was parked for operator/platform action.
     */
    DLQ
}
