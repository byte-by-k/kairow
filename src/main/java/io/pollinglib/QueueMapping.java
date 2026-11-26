package io.pollinglib;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Pattern;

/** Physical, unquoted SQL identifiers. Tables may include a schema prefix. */
public final class QueueMapping {
    private static final List<String> COLUMNS = List.of("id", "payload", "status", "attempts",
            "max_attempts", "available_at", "owner_id", "claim_token");
    private static final Pattern SLOT = Pattern.compile("\\{([a-z_]+)\\}");
    private static final String IDENT = "[A-Za-z_][A-Za-z0-9_]*";
    static final QueueMapping DEFAULT = new QueueMapping("polling_job", "polling_leader", Map.of());
    private Map<String,String> names;

    public QueueMapping(String table, String leaderTable, Map<String,String> columns) {
        var result = new HashMap<String,String>();
        var used = new HashSet<String>();
        if (!COLUMNS.containsAll(columns.keySet())) throw new IllegalArgumentException("Unknown queue column");
        for (String key : COLUMNS) {
            String name = columns.getOrDefault(key, key);
            if (!name.matches(IDENT) || !used.add(name.toLowerCase(Locale.ROOT)))
                throw new IllegalArgumentException("Invalid or duplicate column: " + name);
            result.put(key, name);
        }
        for (String name : List.of(table, leaderTable)) {
            if (!name.matches(IDENT + "(?:\\." + IDENT + ")?"))
                throw new IllegalArgumentException("Invalid table: " + name);
        }
        result.put("table", table);
        result.put("leader", leaderTable);
        names = Map.copyOf(result);
    }

    private boolean ordered;

    String orderGuard() {
        if (!ordered) return "";
        return """
             AND NOT EXISTS (SELECT 1 FROM {table} prior
                 WHERE prior.{aggregate_key}=job.{aggregate_key}
                 AND prior.{sequence}<job.{sequence} AND prior.{status}<>'DONE')
            """;
    }

    static QueueMapping from(KairowPoll a) {
        var mapping = new QueueMapping(a.companionStateTable().isEmpty() ? a.table() : a.companionStateTable(), a.leaderTable(), Map.of(
                "id", a.idColumn(),
                "payload", a.payloadColumn(),
                "status", a.statusColumn(),
                "attempts", a.attemptsColumn(),
                "max_attempts", a.maxAttemptsColumn(),
                "available_at", a.availableAtColumn(),
                "owner_id", a.ownerColumn(),
                "claim_token", a.claimTokenColumn()));
        if (!a.aggregateKeyColumn().isEmpty() || !a.aggregateSequenceColumn().isEmpty()) {
            if (!a.aggregateKeyColumn().matches(IDENT) || !a.aggregateSequenceColumn().matches(IDENT))
                throw new IllegalArgumentException("Supply both aggregate key and sequence column names");
            var names = new HashMap<>(mapping.names);
            names.put("aggregate_key", a.aggregateKeyColumn());
            names.put("sequence", a.aggregateSequenceColumn());
            mapping.names = Map.copyOf(names);
            mapping.ordered = true;
        }
        return mapping;
    }

    /** Validated SQL names by logical key, including table, leader and configured ordering columns. */
    public Map<String,String> identifiers() { return names; }

    public PreparedStatement prepare(Connection tx, String template) throws SQLException {
        return tx.prepareStatement(SLOT.matcher(template).replaceAll(m -> names.get(m.group(1))));
    }
}
