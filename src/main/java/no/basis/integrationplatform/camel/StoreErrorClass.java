package no.basis.integrationplatform.camel;

import java.sql.SQLException;
import java.util.IdentityHashMap;
import java.util.Map;

/**
 * What a failed Message Store call says about the store, read from its SQLSTATE. The adapter's
 * log lines name the class, so an alert can tell a read-only store or a lost grant from an
 * outage, and a record whose own data the store cannot hold takes the terminal path instead of
 * being read again.
 */
public enum StoreErrorClass {
    /** Class 22 or 54000: this record's data cannot be stored; a retry cannot help. */
    RECORD_DATA("record-data"),
    /** 25006: the database is read-only, for example a standby after a failover. */
    READ_ONLY("read-only"),
    /** 42501 or class 28: the login lacks a privilege. */
    PRIVILEGE("privilege"),
    /** Another class 42, class 23, 3D000 or 3F000: the schema or its constraints do not match the adapter. */
    SCHEMA("schema"),
    /** A step's output could not be captured for its checkpoint; there is no outcome to keep. */
    CHECKPOINT("checkpoint"),
    /** Anything else, including a timeout or a broken connection without a SQLSTATE. */
    UNAVAILABLE("unavailable");

    private static final int MAX_CAUSES = 20;
    private final String label;

    StoreErrorClass(String label) {
        this.label = label;
    }

    /**
     * The class as the adapter's log lines name it.
     *
     * @return the label, such as {@code read-only}
     */
    public String label() {
        return label;
    }

    /**
     * Classifies a failure by the outermost {@link SQLException} in its cause chain that has a
     * SQLSTATE.
     *
     * @param failure a failed store call; may be {@code null}
     * @return the class; {@link #UNAVAILABLE} without a SQLSTATE
     */
    public static StoreErrorClass of(Throwable failure) {
        if (capturesOutput(failure)) {
            return CHECKPOINT;
        }
        return ofSqlState(sqlState(failure));
    }

    private static boolean capturesOutput(Throwable failure) {
        Map<Throwable, Boolean> seen = new IdentityHashMap<>();
        Throwable current = failure;
        for (int depth = 0; current != null && depth <= MAX_CAUSES && seen.put(current, true) == null; depth++) {
            if (current instanceof no.basis.integrationplatform.camel.step.StepOutputCheckpointException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    /**
     * The SQLSTATE that {@link #of(Throwable)} classifies by.
     *
     * @param failure a failed store call; may be {@code null}
     * @return the SQLSTATE, or {@code null} when the cause chain has none
     */
    public static String sqlState(Throwable failure) {
        Map<Throwable, Boolean> seen = new IdentityHashMap<>();
        Throwable current = failure;
        for (int depth = 0; current != null && depth <= MAX_CAUSES && seen.put(current, true) == null; depth++) {
            if (current instanceof SQLException sql && sql.getSQLState() != null && !sql.getSQLState().isBlank()) {
                return sql.getSQLState();
            }
            current = current.getCause();
        }
        return null;
    }

    static StoreErrorClass ofSqlState(String sqlState) {
        if (sqlState == null) {
            return UNAVAILABLE;
        }
        if (sqlState.startsWith("22") || sqlState.equals("54000")) {
            return RECORD_DATA;
        }
        if (sqlState.equals("25006")) {
            return READ_ONLY;
        }
        if (sqlState.equals("42501") || sqlState.startsWith("28")) {
            return PRIVILEGE;
        }
        if (sqlState.startsWith("42") || sqlState.startsWith("23") || sqlState.equals("3D000") || sqlState.equals("3F000")) {
            return SCHEMA;
        }
        return UNAVAILABLE;
    }

    /**
     * The end of a log line about a failed store call: {@code storeError=<class>}, and
     * {@code sqlState=<code>} when known.
     *
     * @param failure a failed store call; may be {@code null}
     * @return the text, starting with a space
     */
    public static String logSuffix(Throwable failure) {
        String sqlState = sqlState(failure);
        return " storeError=" + of(failure).label + (sqlState == null ? "" : " sqlState=" + sqlState);
    }
}
