package no.basis.integrationplatform.camel.keyvalue;

/**
 * The application has no Key-Value Store: it has no Message Store datasource, an
 * inactive one, or no {@code app.name} to own the keys. Thrown on the store's first
 * call, when CDI creates it, possibly wrapped by the container. It is an
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

    /**
     * Creates the exception with its cause.
     *
     * @param message what is missing
     * @param cause why it is missing
     */
    public KeyValueStoreNotConfiguredException(String message, Throwable cause) {
        super(message, cause);
    }
}
