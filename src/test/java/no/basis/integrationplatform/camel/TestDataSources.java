package no.basis.integrationplatform.camel;

import jakarta.enterprise.inject.Instance;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;

/** CDI lookups of the default DataSource without a container. */
public final class TestDataSources {
    private TestDataSources() {
    }

    /** Two beans qualify as @Default, as with a second @Named DataSource producer. */
    @SuppressWarnings("unchecked")
    public static Instance<DataSource> ambiguous() {
        return (Instance<DataSource>) Proxy.newProxyInstance(TestDataSources.class.getClassLoader(),
                new Class<?>[]{Instance.class}, (proxy, method, args) -> switch (method.getName()) {
                    case "isAmbiguous" -> true;
                    case "isResolvable", "isUnsatisfied" -> false;
                    case "get" -> throw new jakarta.enterprise.inject.AmbiguousResolutionException("two DataSources");
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
