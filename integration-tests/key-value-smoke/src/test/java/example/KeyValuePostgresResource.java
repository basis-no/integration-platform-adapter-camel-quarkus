package example;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

/** Isolated synthetic database; never accepts an externally supplied JDBC URL. */
public class KeyValuePostgresResource implements QuarkusTestResourceLifecycleManager {
    private PostgreSQLContainer postgres;

    @Override
    public Map<String, String> start() {
        String runId = System.getProperty("bip.test.run-id");
        if (runId == null || runId.isBlank()) {
            throw new IllegalArgumentException("Unique bip.test.run-id is required");
        }
        postgres = new PostgreSQLContainer("postgres@sha256:b0f9560a2de083e2cc7382e75f808c7381a32852a7ec49117deedb300e552b24")
                .withImagePullPolicy(image -> false)
                .withLabel("bip.test", "key-value-smoke")
                .withLabel("bip.qualification.run", runId)
                .withStartupTimeout(Duration.ofSeconds(60))
                .withCreateContainerCmdModifier(command -> command.getHostConfig()
                        .withMemory(256L * 1024 * 1024).withMemorySwap(256L * 1024 * 1024)
                        .withPidsLimit(64L).withNanoCPUs(1_000_000_000L)
                        .withPortBindings(new com.github.dockerjava.api.model.PortBinding(
                                com.github.dockerjava.api.model.Ports.Binding.bindIp("127.0.0.1"),
                                new com.github.dockerjava.api.model.ExposedPort(5432))));
        try {
            postgres.start();
            var source = new PGSimpleDataSource();
            source.setURL(postgres.getJdbcUrl());
            source.setUser(postgres.getUsername());
            source.setPassword(postgres.getPassword());
            try (var connection = source.getConnection(); var statement = connection.createStatement()) {
                for (String resource : new String[]{"/db/message-store/bip_contract.sql", "/db/key-value.sql"}) {
                    try (var input = getClass().getResourceAsStream(resource)) {
                        if (input == null) throw new IllegalStateException("Missing synthetic schema " + resource);
                        statement.execute(new String(input.readAllBytes(), StandardCharsets.UTF_8));
                    }
                }
                if (statement.executeUpdate("insert into bip_contract (contract_major, contract_version) values (1, '1.2.0-rc2')") != 1) {
                    throw new IllegalStateException("Synthetic contract metadata was not seeded");
                }
                statement.execute("insert into bip_key_value(integration, key, value) values ('other-integration', 'cursor/runtime', 'true'::jsonb)");
            }
            return Map.of("quarkus.datasource.jdbc.url", postgres.getJdbcUrl(),
                    "quarkus.datasource.username", postgres.getUsername(),
                    "quarkus.datasource.password", postgres.getPassword());
        } catch (Exception failure) {
            try { postgres.stop(); }
            catch (RuntimeException cleanupFailure) { failure.addSuppressed(cleanupFailure); }
            throw new IllegalStateException("Synthetic Key-Value Store setup failed", failure);
        }
    }

    @Override
    public void stop() {
        // Quarkus must have disposed the producer before its test database stops.
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (sweeperRunning() && System.nanoTime() < deadline) {
            try { Thread.sleep(25); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); break; }
        }
        boolean leaked = sweeperRunning();
        RuntimeException failure = leaked
                ? new IllegalStateException("Key-Value Store sweeper survived application disposal") : null;
        try { if (postgres != null) postgres.stop(); }
        catch (RuntimeException cleanupFailure) {
            if (failure == null) failure = cleanupFailure; else failure.addSuppressed(cleanupFailure);
        }
        if (failure != null) throw failure;
    }

    private boolean sweeperRunning() {
        return Thread.getAllStackTraces().keySet().stream()
                .anyMatch(thread -> thread.isAlive() && thread.getName().equals("bip-key-value-sweep"));
    }
}
