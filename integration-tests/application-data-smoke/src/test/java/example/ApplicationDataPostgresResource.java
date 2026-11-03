package example;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

/** One synthetic server, separate Message Store and existing application databases. */
public class ApplicationDataPostgresResource implements QuarkusTestResourceLifecycleManager {
    private PostgreSQLContainer postgres;
    @Override public Map<String, String> start() {
        String run = System.getProperty("bip.test.run-id");
        if (run == null) throw new IllegalArgumentException("Unique run ID required");
        java.util.UUID.fromString(run);
        postgres = new PostgreSQLContainer("postgres@sha256:b0f9560a2de083e2cc7382e75f808c7381a32852a7ec49117deedb300e552b24")
                .withImagePullPolicy(image -> false).withLabel("bip.qualification.run", run)
                .withStartupTimeout(Duration.ofSeconds(60))
                .withCreateContainerCmdModifier(command -> command.getHostConfig()
                    .withMemory(256L * 1024 * 1024).withMemorySwap(256L * 1024 * 1024)
                    .withPidsLimit(64L).withNanoCPUs(1_000_000_000L)
                    .withPortBindings(new com.github.dockerjava.api.model.PortBinding(
                        com.github.dockerjava.api.model.Ports.Binding.bindIp("127.0.0.1"),
                        new com.github.dockerjava.api.model.ExposedPort(5432))));
        try {
            postgres.start();
            var message = source(postgres.getJdbcUrl());
            try (var connection = message.getConnection(); var statement = connection.createStatement()) {
                try (var input = getClass().getResourceAsStream("/db/message-store/bip_contract.sql")) {
                    if (input == null) throw new IllegalStateException("Missing adapter contract metadata fixture");
                    statement.execute(new String(input.readAllBytes(), StandardCharsets.UTF_8));
                }
                statement.executeUpdate("insert into bip_contract(contract_major, contract_version) values (1, '1.0.0-rc2')");
                statement.execute("create database application_synthetic");
            }
            String applicationUrl = postgres.getJdbcUrl().replace("/" + postgres.getDatabaseName(), "/application_synthetic");
            if (applicationUrl.equals(postgres.getJdbcUrl())) throw new IllegalStateException("Application database must be separate");
            try (var connection = source(applicationUrl).getConnection(); var statement = connection.createStatement()) {
                statement.execute("create table existing_application_cursor(key text primary key, next_date date not null)");
                statement.executeUpdate("insert into existing_application_cursor values ('synthetic', date '2026-01-01')");
            }
            return Map.of("quarkus.datasource.jdbc.url", postgres.getJdbcUrl(),
                    "quarkus.datasource.username", postgres.getUsername(), "quarkus.datasource.password", postgres.getPassword(),
                    "quarkus.datasource.application.jdbc.url", applicationUrl,
                    "quarkus.datasource.application.username", postgres.getUsername(), "quarkus.datasource.application.password", postgres.getPassword());
        } catch (Exception failure) {
            try { postgres.stop(); } catch (RuntimeException cleanup) { failure.addSuppressed(cleanup); }
            throw new IllegalStateException("Synthetic application database fixture failed", failure);
        }
    }
    private PGSimpleDataSource source(String url) {
        var source = new PGSimpleDataSource(); source.setURL(url);
        source.setUser(postgres.getUsername()); source.setPassword(postgres.getPassword()); return source;
    }
    @Override public void stop() { if (postgres != null) postgres.stop(); }
}
