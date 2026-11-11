package no.basis.integrationplatform.camel;

import jakarta.enterprise.inject.Instance;

import javax.sql.DataSource;

/**
 * Resolves the application's default JDBC DataSource for the Message Store, step
 * outcomes and the Key-Value Store.
 */
public final class MessageStoreDataSource {
    private MessageStoreDataSource() {
    }

    /**
     * Returns the default DataSource.
     * @param dataSource the injected default DataSource
     * @param requiredBy the setting or feature that needs it, for the error message
     * @return the DataSource
     * @throws IllegalStateException when no DataSource, or more than one, qualifies
     */
    public static DataSource require(Instance<DataSource> dataSource, String requiredBy) {
        if (dataSource.isResolvable()) {
            return dataSource.get();
        }
        if (dataSource.isAmbiguous()) {
            throw ambiguous(requiredBy);
        }
        throw new IllegalStateException(requiredBy + " requires a JDBC DataSource");
    }

    /**
     * The error for more than one default DataSource.
     * @param requiredBy the setting or feature that needs it
     * @return the error
     */
    public static IllegalStateException ambiguous(String requiredBy) {
        // A @Named producer is also @Default, so a second DataSource handed to Camel
        // that way makes the lookup ambiguous.
        return new IllegalStateException(requiredBy + " requires the application's default JDBC DataSource, but more "
                + "than one DataSource bean is @Default. A @Named DataSource producer is also @Default; give the "
                + "additional DataSource a qualifier such as @Identifier(\"<name>\") so that only the Message Store "
                + "DataSource is @Default");
    }
}
