package no.basis.integrationplatform.camel.lease;

import no.basis.integrationplatform.camel.keyvalue.KeyValueStoreException;
import no.basis.integrationplatform.camel.keyvalue.KeyValueStoreNotConfiguredException;

import java.sql.SQLException;

/** How the lease protocol reads the Key-Value Store's failures. */
final class KeyValueStores {
    /** The Key-Value Store's table, named in log messages. */
    static final String TABLE = "bip_key_value";

    private static final String UNDEFINED_TABLE = "42P01";

    private KeyValueStores() {
    }

    /**
     * Returns the reason the application has no Key-Value Store, found anywhere in the failure's
     * cause chain, or {@code null} when the failure is something else.
     */
    static KeyValueStoreNotConfiguredException notConfigured(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof KeyValueStoreNotConfiguredException notConfigured) {
                return notConfigured;
            }
        }
        return null;
    }

    /**
     * Whether the failure means the Message Store has no Key-Value Store table, rather than an
     * unreachable store: the gate then stays open with a warning instead of closing.
     */
    static boolean notProvisioned(KeyValueStoreException failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof SQLException sql && UNDEFINED_TABLE.equals(sql.getSQLState())) {
                return true;
            }
        }
        return false;
    }
}
