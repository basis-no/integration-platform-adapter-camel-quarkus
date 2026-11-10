package no.basis.integrationplatform.camel.lease;

import com.fasterxml.jackson.databind.JsonNode;
import no.basis.integrationplatform.camel.keyvalue.KeyValueStore;
import no.basis.integrationplatform.camel.keyvalue.KeyValueEntry;
import no.basis.integrationplatform.camel.keyvalue.KeyValueKey;
import org.apache.camel.spi.IdempotentRepository;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/** Forwards every operation to a real store so a test can observe or interrupt single calls. */
class DelegatingKeyValueStore implements KeyValueStore {
    private final KeyValueStore delegate;

    DelegatingKeyValueStore(KeyValueStore delegate) {
        this.delegate = delegate;
    }

    protected void before(String operation) {
    }

    @Override
    public Optional<KeyValueEntry> get(String key) {
        before("get");
        return delegate.get(key);
    }

    @Override
    public long put(String key, JsonNode value, Duration ttl) {
        before("put");
        return delegate.put(key, value, ttl);
    }

    @Override
    public OptionalLong compareAndSet(String key, long expectedVersion, JsonNode value, Duration ttl) {
        before("compareAndSet");
        return delegate.compareAndSet(key, expectedVersion, value, ttl);
    }

    @Override
    public boolean putIfAbsent(String key, JsonNode value, Duration ttl) {
        before("putIfAbsent");
        return delegate.putIfAbsent(key, value, ttl);
    }

    @Override
    public boolean delete(String key) {
        before("delete");
        return delegate.delete(key);
    }

    @Override
    public boolean delete(String key, long expectedVersion) {
        before("delete");
        return delegate.delete(key, expectedVersion);
    }

    @Override
    public List<KeyValueKey> list(String prefix, String after, int limit) {
        before("list");
        return delegate.list(prefix, after, limit);
    }

    @Override
    public IdempotentRepository idempotentRepository(String prefix, Duration ttl) {
        return delegate.idempotentRepository(prefix, ttl);
    }
}
