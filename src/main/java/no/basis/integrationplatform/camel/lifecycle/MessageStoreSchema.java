package no.basis.integrationplatform.camel.lifecycle;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The tables and columns this adapter writes, read from the schema it ships in
 * {@code db/message-store/}, and their check against a Message Store.
 */
final class MessageStoreSchema {
    static final String MESSAGE_STORE_RESOURCE = "/db/message-store/bip_message_store.sql";
    static final String STEP_OUTCOME_RESOURCE = "/db/message-store/bip_step_outcome.sql";

    // pg_attribute lists every column of a table the search path resolves, also one the
    // role has no privilege on; to_regclass gives null for a table that does not exist.
    static final String COLUMNS_SQL = """
            select t.name, a.attname
            from unnest(?) as t(name)
            left join pg_attribute a
              on a.attrelid = to_regclass(t.name) and a.attnum > 0 and not a.attisdropped
            """;

    private static final Pattern CREATE_TABLE = Pattern.compile(
            "create\\s+table\\s+(?:if\\s+not\\s+exists\\s+)?([a-z_][a-z0-9_]*)\\s*\\(", Pattern.CASE_INSENSITIVE);
    private static final Set<String> TABLE_CONSTRAINTS = Set.of("primary", "constraint", "unique", "check", "foreign", "exclude");

    private final Map<String, String> sources = new LinkedHashMap<>();
    private final Map<String, Map<String, String>> tables = new LinkedHashMap<>();

    private MessageStoreSchema() {
    }

    /**
     * The tables of the shipped schema files that the adapter writes in this configuration.
     *
     * @param messageStore the message tables, written by the Postgres sink
     * @param replayAudit the replay audit table, written when a message is replayed
     * @param stepOutcomes the step outcome table, written by the Postgres step store
     */
    static MessageStoreSchema required(boolean messageStore, boolean replayAudit, boolean stepOutcomes) {
        MessageStoreSchema schema = new MessageStoreSchema();
        Map<String, Map<String, String>> store = parse(resource(MESSAGE_STORE_RESOURCE));
        if (messageStore) {
            schema.add(MESSAGE_STORE_RESOURCE, "bip_message_log", store);
            schema.add(MESSAGE_STORE_RESOURCE, "bip_message_attempt", store);
        }
        if (replayAudit) {
            schema.add(MESSAGE_STORE_RESOURCE, "bip_message_replay_audit", store);
        }
        if (stepOutcomes) {
            schema.add(STEP_OUTCOME_RESOURCE, "bip_step_outcome", parse(resource(STEP_OUTCOME_RESOURCE)));
        }
        return schema;
    }

    boolean isEmpty() {
        return tables.isEmpty();
    }

    Map<String, Map<String, String>> tables() {
        return tables;
    }

    /**
     * Checks in one catalog query that every table and column exists.
     *
     * @throws IllegalStateException naming what is missing and how to add it
     */
    void verify(Connection connection) throws SQLException {
        Map<String, Set<String>> present = new HashMap<>();
        Array names = connection.createArrayOf("text", tables.keySet().toArray());
        try (PreparedStatement statement = connection.prepareStatement(COLUMNS_SQL)) {
            statement.setArray(1, names);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    Set<String> columns = present.computeIfAbsent(resultSet.getString(1), table -> new HashSet<>());
                    String column = resultSet.getString(2);
                    if (column != null) {
                        columns.add(column);
                    }
                }
            }
        } finally {
            names.free();
        }
        List<String> missing = new ArrayList<>();
        List<String> statements = new ArrayList<>();
        tables.forEach((table, columns) -> {
            Set<String> found = present.getOrDefault(table, Set.of());
            if (found.isEmpty()) {
                missing.add("table " + table);
                statements.add("create " + table + " with " + sources.get(table).substring(1));
                return;
            }
            List<String> absent = columns.keySet().stream().filter(column -> !found.contains(column)).toList();
            if (!absent.isEmpty()) {
                missing.add(table + "." + String.join(", " + table + ".", absent));
                absent.forEach(column -> statements.add(
                        "alter table " + table + " add column if not exists " + columns.get(column)));
            }
        });
        if (!missing.isEmpty()) {
            throw new IllegalStateException("The Message Store lacks " + String.join(", ", missing)
                    + ", which this adapter writes. Refusing to start: every message outcome would fail to record,"
                    + " and its Kafka record would be read and processed again. The store was provisioned for an"
                    + " earlier adapter version. Bring it to the schema in db/message-store/ in the adapter JAR"
                    + " before starting: " + String.join("; ", statements) + ".");
        }
    }

    private void add(String source, String table, Map<String, Map<String, String>> parsed) {
        Map<String, String> columns = parsed.get(table);
        if (columns == null || columns.isEmpty()) {
            throw new IllegalStateException(source + " in the adapter JAR does not define table " + table);
        }
        sources.put(table, source);
        tables.put(table, columns);
    }

    /** Column name to its definition, per table, from the {@code create table} statements. */
    static Map<String, Map<String, String>> parse(String sql) {
        String text = sql.replaceAll("--[^\\n]*", "");
        Map<String, Map<String, String>> result = new LinkedHashMap<>();
        Matcher create = CREATE_TABLE.matcher(text);
        while (create.find()) {
            int start = create.end();
            int depth = 1;
            int end = start;
            while (depth > 0 && end < text.length()) {
                char c = text.charAt(end++);
                if (c == '(') depth++;
                if (c == ')') depth--;
            }
            Map<String, String> columns = new LinkedHashMap<>();
            for (String part : topLevelParts(text.substring(start, end - 1))) {
                String definition = part.trim().replaceAll("\\s+", " ");
                if (definition.isEmpty()) {
                    continue;
                }
                String name = definition.split(" ", 2)[0].toLowerCase(Locale.ROOT);
                if (!TABLE_CONSTRAINTS.contains(name)) {
                    columns.put(name, definition);
                }
            }
            result.put(create.group(1).toLowerCase(Locale.ROOT), columns);
        }
        return result;
    }

    private static Collection<String> topLevelParts(String body) {
        List<String> parts = new ArrayList<>();
        int depth = 0;
        int from = 0;
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            if (c == '(') depth++;
            if (c == ')') depth--;
            if (c == ',' && depth == 0) {
                parts.add(body.substring(from, i));
                from = i + 1;
            }
        }
        parts.add(body.substring(from));
        return parts;
    }

    private static String resource(String name) {
        try (InputStream input = MessageStoreSchema.class.getResourceAsStream(name)) {
            if (input == null) {
                throw new IllegalStateException("The adapter JAR lacks its schema resource " + name);
            }
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("The adapter cannot read its schema resource " + name, e);
        }
    }
}
