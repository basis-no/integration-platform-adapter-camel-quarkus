package no.basis.integrationplatform.camel.keyvalue;

import javax.sql.DataSource;

/** Opens the package-private Postgres Key-Value Store to tests in other packages. */
public final class TestKeyValueStores {
    private TestKeyValueStores() {
    }

    public static KeyValueStore postgres(DataSource dataSource, String integration) {
        return new PostgresKeyValueStore(dataSource, integration);
    }
}
