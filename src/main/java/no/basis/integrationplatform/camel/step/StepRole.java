package no.basis.integrationplatform.camel.step;

/** Replay semantics assigned to an intercepted Camel endpoint send. */
public enum StepRole {
    /** Route mechanics that are not a business operation. */
    INTERNAL,
    /** An operation that is expected to run again on every replay. */
    REPEATABLE,
    /** A durable external write protected by the delivered-step ledger. */
    SIDE_EFFECT,
    /** A durable external write whose response is required by later route logic. */
    OUTPUT_SIDE_EFFECT,
    /** An operation that executes untracked until its replay semantics are explicit. */
    AMBIGUOUS
}
