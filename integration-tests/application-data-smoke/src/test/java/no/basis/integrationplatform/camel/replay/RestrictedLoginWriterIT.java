package no.basis.integrationplatform.camel.replay;

import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.model.StepStatus;
import no.basis.integrationplatform.camel.step.PostgresStepOutcomeStore;
import no.basis.integrationplatform.camel.store.PostgresMessageStoreSink;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;
import java.nio.file.*;
import java.security.MessageDigest;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.jar.JarFile;

import static org.junit.jupiter.api.Assertions.*;

/** Actual adapter calls; plain JUnit, no Quarkus application or broker startup. */
@Timeout(120)
class RestrictedLoginWriterIT {
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) static Path temporary;
    static PostgreSQLContainer postgres;
    static PGSimpleDataSource admin, runtime, operator, reader;
    static final Instant AT = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
    static final Instant EXPIRY = AT.plus(Duration.ofDays(30));
    static final String INTEGRATION = "synthetic-orders";
    static final String RUNTIME = "fixture_runtime", OPERATOR = "fixture_operator", READER = "fixture_reader";
    static final String TABLES = "public.bip_message_log, public.bip_message_attempt, public.bip_message_replay_audit, public.bip_step_outcome";

    @BeforeAll @Timeout(120) static void installOwnedDatabase() throws Exception {
        UUID.fromString(required("bip.test.run-id"));
        verifyLoadedArtifact();
        Path sql = Path.of(required("bip.test.recovery-sql-dir")).toRealPath();
        for (String file : List.of("002_adapter_v1_message_store.sql", "011_message_recovery.sql", "012_message_recovery_grants.psql"))
            assertEquals(required("bip.test.sha256." + file), sha(sql.resolve(file)));
        Path psql = Path.of(required("bip.test.psql")).toRealPath();
        assertTrue(psql.isAbsolute() && Files.isExecutable(psql));
        assertEquals(required("bip.test.psql-sha256"), sha(psql));
        postgres = new PostgreSQLContainer("postgres@sha256:b0f9560a2de083e2cc7382e75f808c7381a32852a7ec49117deedb300e552b24")
                .withImagePullPolicy(image -> false)
                .withLabel("bip.qualification.run", required("bip.test.run-id"))
                .withStartupTimeout(Duration.ofSeconds(60))
                .withCreateContainerCmdModifier(command -> command.getHostConfig()
                        .withMemory(256L * 1024 * 1024).withMemorySwap(256L * 1024 * 1024)
                        .withPidsLimit(64L).withNanoCPUs(1_000_000_000L)
                        .withPortBindings(new com.github.dockerjava.api.model.PortBinding(
                                com.github.dockerjava.api.model.Ports.Binding.bindIp("127.0.0.1"),
                                new com.github.dockerjava.api.model.ExposedPort(5432))));
        try {
            postgres.start();
            assertTrue(Set.of("localhost", "127.0.0.1").contains(postgres.getHost()), "Fixture must use the local owned engine");
            admin = source(postgres.getUsername(), postgres.getPassword());
            try (var c = admin.getConnection(); var s = c.createStatement()) {
                s.execute(Files.readString(sql.resolve("002_adapter_v1_message_store.sql")));
                s.execute(Files.readString(sql.resolve("011_message_recovery.sql")));
                s.execute("revoke create on schema public from public");
                String password = UUID.randomUUID().toString();
                for (String role : List.of(RUNTIME, OPERATOR, READER))
                    s.execute("create role " + role + " login nosuperuser nocreatedb nocreaterole noinherit noreplication nobypassrls password '" + password + "'");
                s.execute("grant select,insert,update,delete on " + TABLES + " to " + RUNTIME);
                s.execute("grant select on " + TABLES + " to " + READER);
                runtime = source(RUNTIME, password); operator = source(OPERATOR, password); reader = source(READER, password);
            }
            installExactGrantScript(psql, sql.resolve("012_message_recovery_grants.psql"));
            try (var c = admin.getConnection(); var s = c.createStatement()) {
                s.execute("select bip_recovery.check_roles('" + RUNTIME + "','" + OPERATOR + "','" + READER + "')");
            }
        } catch (Exception | Error failed) {
            try { postgres.stop(); } catch (RuntimeException cleanup) { failed.addSuppressed(cleanup); }
            throw failed;
        }
    }

    @AfterAll @Timeout(30) static void stopOwnedDatabase() { if (postgres != null) postgres.stop(); }

    @Test void actualLoginRolesKeepRuntimeOperatorAndReaderAuthoritiesSeparate() throws Exception {
        for (var role : Map.of(RUNTIME, runtime, OPERATOR, operator, READER, reader).entrySet()) {
            try (var c = role.getValue().getConnection(); var s = c.createStatement();
                 var rows = s.executeQuery("select session_user,current_user,rolsuper,rolbypassrls,rolcreaterole from pg_roles where rolname=session_user")) {
                assertTrue(rows.next()); assertEquals(role.getKey(), rows.getString(1)); assertEquals(role.getKey(), rows.getString(2));
                assertFalse(rows.getBoolean(3)); assertFalse(rows.getBoolean(4)); assertFalse(rows.getBoolean(5));
            }
        }
        for (var source : List.of(runtime, reader)) {
            denied(source, "set role " + OPERATOR, "42501");
            denied(source, "select bip_recovery.apply(null,null,null,null,null,null,null,null,null,null)", "42501");
        }
        denied(operator, "update bip_message_log set status='COMPLETED'", "42501");
        denied(reader, "delete from bip_message_attempt", "42501");
        denied(runtime, "set session_replication_role=replica", "42501");
        denied(runtime, "create table public.forbidden_fixture_table(id integer)", "42501");
        assertEquals("true", scalar(reader, "select to_regclass('public.bip_key_value') is null"));
    }

    @Test void actualWriterKeepsClosureAndHistoryAcrossLateCallbacksThenExplicitReopen() throws Exception {
        String id = message(); var sink = new PostgresMessageStoreSink(runtime);
        sink.record(result(id, 2, MessageStatus.DLQ, EXPIRY));
        var steps = new PostgresStepOutcomeStore(runtime);
        steps.recordDelivered(new StepResult("send", StepStatus.DELIVERED, id + ":send", "direct:synthetic", null, null, AT));
        apply(id, "close", false);
        String before = scalar(reader, "select row_to_json(m)::text from bip_message_log m where message_id='" + id + "'");
        sink.record(result(id, 1, MessageStatus.COMPLETED, EXPIRY.plus(Duration.ofDays(1))));
        assertEquals(before, scalar(reader, "select row_to_json(m)::text from bip_message_log m where message_id='" + id + "'"));
        assertEquals("true", scalar(reader, "select closed from bip_message_recovery where message_id='" + id + "'"));
        assertEquals("2", scalar(reader, "select count(*) from bip_message_attempt where message_id='" + id + "'"));
        assertTrue(steps.wasDelivered(id + ":send"));
        assertEquals("true", scalar(reader, "select bool_and(payload_expires_at='" + EXPIRY + "'::timestamptz) from bip_message_attempt where message_id='" + id + "'"));
        sink.record(result(id, 3, MessageStatus.FAILED, EXPIRY));
        assertEquals("3", scalar(reader, "select last_attempt from bip_message_log where message_id='" + id + "'"));
        assertEquals("false", scalar(reader, "select closed from bip_message_recovery where message_id='" + id + "'"));
        apply(id, "close", false);
        assertEquals("true", scalar(reader, "select closed from bip_message_recovery where message_id='" + id + "'"));
        apply(id, "reopen", false);
        assertEquals("false", scalar(reader, "select closed from bip_message_recovery where message_id='" + id + "'"));
    }

    @Test void actualWriterCannotResurrectRemovedPayloadOrEraseHistoryAndDeliveredIdentity() throws Exception {
        String id = message(); var sink = new PostgresMessageStoreSink(runtime);
        sink.record(result(id, 1, MessageStatus.DLQ, EXPIRY));
        var steps = new PostgresStepOutcomeStore(runtime);
        steps.recordDelivered(new StepResult("send", StepStatus.DELIVERED, id + ":send", "direct:synthetic", null, null, AT));
        apply(id, "remove_payload", true);
        sink.record(result(id, 1, MessageStatus.COMPLETED, EXPIRY.plus(Duration.ofDays(3))));
        sink.record(result(id, 2, MessageStatus.DLQ, EXPIRY.plus(Duration.ofDays(3))));
        assertRedacted(id);
        assertEquals("2", scalar(reader, "select count(*) from bip_message_attempt where message_id='" + id + "'"));
        assertTrue(steps.wasDelivered(id + ":send"));
        denied(runtime, "delete from bip_message_attempt where message_id='" + id + "'", "55000");
        assertEquals("2", scalar(reader, "select count(*) from bip_message_attempt where message_id='" + id + "'"));
        String fresh = message(); sink.record(result(fresh, 1, MessageStatus.DLQ, EXPIRY));
        assertEquals("false", scalar(reader, "select closed from bip_message_recovery where message_id='" + fresh + "'"));
    }

    @Test void actualReplayClientGetsClosedAdmissionRefusalAndRemovedPayloadRefusal() throws Exception {
        String id = message(); new PostgresMessageStoreSink(runtime).record(result(id, 1, MessageStatus.DLQ, EXPIRY));
        try (var camel = new DefaultCamelContext()) {
            camel.addRoutes(new RouteBuilder() { public void configure() {
                from("kafka:bip.synthetic-orders.in?brokers=127.0.0.1:1").routeId("fixture-input").autoStartup(false).process(e -> {});
            }});
            camel.start(); // Auto-start is false: never connect a consumer or start a broker.
            // Empty bootstrap is an independent fail-safe if an unexpected admission succeeds.
            var service = new PostgresKafkaMessageReplayService(runtime, "", "bip.%s.in", camel, INTEGRATION);
            apply(id, "close", false);
            var closed = assertThrows(MessageReplayException.class, () -> service.replay(id));
            assertSqlCause(closed, "55000", "RECOVERY_SOURCE_CLOSED_OR_REMOVED");
            assertEquals("0", scalar(reader, "select count(*) from bip_message_replay_audit where message_id='" + id + "'"));
            apply(id, "reopen", false);
            apply(id, "remove_payload", true);
            var removed = assertThrows(MessageReplayException.class, () -> service.replay(id));
            assertTrue(removed.getMessage().contains("not replayable"));
            assertEquals("0", scalar(reader, "select count(*) from bip_message_replay_audit where message_id='" + id + "'"));
        }
    }

    @Test void acceptedUncertainReplayRefusesPayloadRemovalWithoutChangingCustody() throws Exception {
        String id = message(); new PostgresMessageStoreSink(runtime).record(result(id, 1, MessageStatus.DLQ, EXPIRY));
        try (var c = runtime.getConnection(); var s = c.prepareStatement("insert into bip_message_replay_audit(replay_id,message_id,integration,replay_attempt,target_topic,status,requested_at) values(?::uuid,?, ?,2,'bip.synthetic-orders.in','REQUESTED',now())")) {
            s.setString(1, UUID.randomUUID().toString()); s.setString(2, id); s.setString(3, INTEGRATION); s.executeUpdate();
        }
        var refused = assertThrows(SQLException.class, () -> apply(id, "remove_payload", true));
        assertEquals("55000", refused.getSQLState()); assertTrue(refused.getMessage().contains("RECOVERY_IN_FLIGHT_OR_UNCERTAIN"));
        assertEquals("true", scalar(reader, "select payload_bytes is not null and payload_deleted_at is null from bip_message_log where message_id='" + id + "'"));
    }

    @Test void actualCallbackBlockedBehindOperatorRemovalResumesWithRedactedHistory() throws Exception {
        String id = message(); var sink = new PostgresMessageStoreSink(runtime);
        sink.record(result(id, 1, MessageStatus.DLQ, EXPIRY));
        var worker = Executors.newSingleThreadExecutor(Thread.ofPlatform().daemon(true).name("restricted-writer-fixture").factory());
        Future<?> callback = null;
        try (var c = operator.getConnection()) {
            c.setAutoCommit(false);
            apply(c, id, "remove_payload", true);
            callback = worker.submit(() -> sink.record(result(id, 1, MessageStatus.COMPLETED, EXPIRY.plus(Duration.ofDays(3)))));
            long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
            while (!"true".equals(scalar(admin, "select exists(select 1 from pg_stat_activity where application_name='" + RUNTIME + "' and cardinality(pg_blocking_pids(pid))>0)"))) {
                if (System.nanoTime() >= deadline) fail("Actual JDBC callback never reached the row-lock barrier");
                Thread.sleep(10);
            }
            c.commit();
            callback.get(10, TimeUnit.SECONDS);
            assertRedacted(id);
        } finally {
            if (callback != null && !callback.isDone()) callback.cancel(true);
            worker.shutdownNow(); assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS), "Owned JDBC callback did not stop");
        }
    }

    static void assertRedacted(String id) throws Exception {
        for (String table : List.of("bip_message_log", "bip_message_attempt"))
            assertEquals("true", scalar(reader, "select bool_and(payload_bytes is null and payload_ref is null and payload_storage='metadata_only' and payload_deleted_at is not null and not replayable and payload_expires_at='" + EXPIRY + "'::timestamptz) from " + table + " where message_id='" + id + "'"));
    }

    static String apply(String id, String operation, boolean confirm) throws Exception {
        try (var c = operator.getConnection()) { return apply(c, id, operation, confirm); }
    }
    static String apply(Connection authority, String id, String operation, boolean confirm) throws Exception {
        try (var c = reader.getConnection(); var read = c.prepareStatement("select m.last_attempt,m.updated_at,r.revision from bip_message_log m join bip_message_recovery r using(message_id,integration) where m.message_id=?")) {
            read.setString(1, id);
            try (var row = read.executeQuery()) {
                assertTrue(row.next());
                try (var call = authority.prepareStatement("select bip_recovery.apply(?::uuid,?,?,?,?,?,?, 'fixture-operator','Synthetic JDBC qualification',?)")) {
                    call.setString(1, UUID.randomUUID().toString()); call.setString(2, INTEGRATION); call.setString(3, id); call.setString(4, operation);
                    call.setInt(5, row.getInt(1)); call.setTimestamp(6, row.getTimestamp(2)); call.setLong(7, row.getLong(3)); call.setBoolean(8, confirm);
                    try (var result = call.executeQuery()) { assertTrue(result.next()); return result.getString(1); }
                }
            }
        }
    }

    static MessageAttemptResult result(String id, int attempt, MessageStatus status, Instant expiry) {
        return new MessageAttemptResult(1,id,"synthetic-correlation","application/json","synthetic","receiver","postgres_inline",2L,
                "{}".getBytes(java.nio.charset.StandardCharsets.UTF_8),expiry,null,true,null,INTEGRATION,"fixture-input",attempt,false,0,0,AT,status,List.of(),"synthetic outcome");
    }
    static String message() { return UUID.randomUUID().toString(); }
    static PGSimpleDataSource source(String role, String password) {
        var source = new PGSimpleDataSource(); source.setURL(postgres.getJdbcUrl()); source.setUser(role); source.setPassword(password);
        source.setApplicationName(role); source.setConnectTimeout(2); source.setSocketTimeout(10);
        source.setOptions("-c statement_timeout=5000 -c lock_timeout=5000"); return source;
    }
    static String scalar(DataSource source, String sql) throws Exception {
        try (var c = source.getConnection(); var s = c.createStatement(); var rows = s.executeQuery(sql)) {
            assertTrue(rows.next());
            Object value = rows.getObject(1);
            return value instanceof Boolean flag ? flag.toString() : rows.getString(1);
        }
    }
    static void denied(DataSource source, String sql, String state) {
        var error = assertThrows(SQLException.class, () -> { try (var c = source.getConnection(); var s = c.createStatement()) { s.execute(sql); }});
        assertEquals(state, error.getSQLState());
    }
    static void assertSqlCause(Throwable error, String state, String marker) {
        for (Throwable cause = error; cause != null; cause = cause.getCause())
            if (cause instanceof SQLException sql && state.equals(sql.getSQLState()) && sql.getMessage().contains(marker)) return;
        fail("Missing exact SQL admission refusal");
    }
    static String required(String key) { return Objects.requireNonNull(System.getProperty(key), "Required qualification input: " + key); }
    static String sha(Path path) throws Exception {
        var digest = MessageDigest.getInstance("SHA-256");
        try (var input = Files.newInputStream(path)) {
            byte[] buffer = new byte[8192]; int length;
            while ((length = input.read(buffer)) != -1) digest.update(buffer, 0, length);
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    static void verifyLoadedArtifact() throws Exception {
        String version = required("bip.test.adapter-version");
        assertTrue(Set.of("0.3.5", "0.3.6-SNAPSHOT").contains(version));
        Path jar = Path.of(PostgresMessageStoreSink.class.getProtectionDomain().getCodeSource().getLocation().toURI()).toRealPath();
        assertTrue(Files.isRegularFile(jar) && jar.toString().endsWith(".jar"), "Use exact packaged adapter bytes, never target/classes");
        assertEquals(required("bip.test.adapter-sha256"), sha(jar));
        assertEquals(PostgresMessageStoreSink.class.getProtectionDomain().getCodeSource().getLocation(), PostgresKafkaMessageReplayService.class.getProtectionDomain().getCodeSource().getLocation());
        assertEquals(PostgresMessageStoreSink.class.getProtectionDomain().getCodeSource().getLocation(), PostgresStepOutcomeStore.class.getProtectionDomain().getCodeSource().getLocation());
        try (var archive = new JarFile(jar.toFile())) {
            var properties = new Properties();
            try (var input = archive.getInputStream(Objects.requireNonNull(archive.getJarEntry("META-INF/maven/no.basis.integrationplatform/integration-platform-adapter-camel-quarkus/pom.properties")))) { properties.load(input); }
            assertEquals(version, properties.getProperty("version"));
        }
        Files.writeString(temporary.resolve("artifact-provenance.txt"), "version=" + version + "\nsha256=" + sha(jar) + "\n");
    }

    static void installExactGrantScript(Path executable, Path script) throws Exception {
        var builder = new ProcessBuilder(executable.toString(), "-X", "--no-password", "-v", "ON_ERROR_STOP=1",
                "-v", "runtime_user=" + RUNTIME, "-v", "operator_user=" + OPERATOR, "-v", "reader_user=" + READER,
                "-v", "integration=" + INTEGRATION, "-f", script.toString());
        builder.environment().clear();
        builder.environment().putAll(Map.of("PATH","/usr/bin:/bin","HOME",temporary.toString(),"PGHOST","127.0.0.1",
                "PGPORT",Integer.toString(postgres.getMappedPort(5432)),"PGDATABASE",postgres.getDatabaseName(),
                "PGUSER",postgres.getUsername(),"PGPASSWORD",postgres.getPassword(),"PGCONNECT_TIMEOUT","2",
                "PGOPTIONS","-c statement_timeout=5000 -c lock_timeout=2000"));
        builder.redirectErrorStream(true).redirectOutput(temporary.resolve("grant-script.log").toFile());
        var process = builder.start(); boolean forced = false;
        try { assertTrue(process.waitFor(20,TimeUnit.SECONDS), "Exact grant script timed out"); assertEquals(0,process.exitValue()); }
        finally {
            if (process.isAlive()) { forced = true; process.destroyForcibly(); }
            assertTrue(process.waitFor(2,TimeUnit.SECONDS), "Owned psql was not reaped");
            assertFalse(forced, "Forced psql cleanup is not qualification");
        }
    }
}
