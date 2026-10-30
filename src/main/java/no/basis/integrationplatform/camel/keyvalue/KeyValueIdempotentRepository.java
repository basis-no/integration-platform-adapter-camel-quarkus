package no.basis.integrationplatform.camel.keyvalue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import org.apache.camel.spi.IdempotentRepository;
import org.apache.camel.support.service.ServiceSupport;

import java.time.Duration;

/** Camel idempotent repository whose seen ids are expiring key-value store keys. */
final class KeyValueIdempotentRepository extends ServiceSupport implements IdempotentRepository {
    private static final JsonNode SEEN = BooleanNode.TRUE;

    private final PostgresKeyValueStore state;
    private final String keyPrefix;
    private final Duration ttl;

    KeyValueIdempotentRepository(PostgresKeyValueStore state, String keyPrefix, Duration ttl) {
        this.state = state;
        this.keyPrefix = keyPrefix;
        this.ttl = ttl;
    }

    @Override
    public boolean add(String key) {
        return state.putIfAbsent(keyPrefix + key, SEEN, ttl);
    }

    @Override
    public boolean contains(String key) {
        return state.get(keyPrefix + key).isPresent();
    }

    @Override
    public boolean remove(String key) {
        return state.delete(keyPrefix + key);
    }

    @Override
    public boolean confirm(String key) {
        // add() already stored the key durably.
        return true;
    }

    @Override
    public void clear() {
        state.deletePrefix(keyPrefix);
    }
}
