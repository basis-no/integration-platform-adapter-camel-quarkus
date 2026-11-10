package no.basis.integrationplatform.camel.keyvalue;

/**
 * The application has no Key-Value Store: it has no Message Store datasource, or no
 * {@code app.name} to own the keys. Thrown when the store is first created. It is an
 * {@link IllegalStateException}, so code that caught that before still does.
 */
public class KeyValueStoreNotConfiguredException extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    /**
     * Creates the exception.
     *
     * @param message what is missing
     */
    public KeyValueStoreNotConfiguredException(String message) {
        super(message);
    }
}
