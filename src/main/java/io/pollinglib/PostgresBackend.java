package io.pollinglib;

import java.sql.*;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import io.pollinglib.PollingQueue.Claim;

final class PostgresBackend implements QueueBackend {
    private final QueueMapping mapping;

    PostgresBackend(QueueMapping mapping) {
        this.mapping = mapping;
    }

    /** Uses the caller's transaction, so enqueue can commit with business data. */
    public void enqueue(Connection tx, UUID id, String payload, int maxAttempts) throws SQLException {
        try (var statement = mapping.prepare(tx,
                "INSERT INTO {table} ({id}, {payload}, {max_attempts}) VALUES (?, ?, ?)")) {
            statement.setObject(1, id);
            statement.setString(2, payload);
            statement.setInt(3, maxAttempts);
            statement.executeUpdate();
        }
    }

    public boolean acquireLeadership(Connection connection, UUID leaderToken, Duration leaderLease) throws SQLException {
        String sql = """
            INSERT INTO {leader} AS current_leader (queue_name, token, expires_at)
            VALUES ('{table}', ?, clock_timestamp() + ? * interval '1 millisecond')
            ON CONFLICT (queue_name) DO UPDATE SET token = excluded.token,
                expires_at = clock_timestamp() + ? * interval '1 millisecond'
            WHERE current_leader.token = excluded.token OR current_leader.expires_at <= clock_timestamp()
            """;
        try (var statement = mapping.prepare(connection, sql)) {
            statement.setObject(1, leaderToken);
            statement.setLong(2, leaderLease.toMillis());
            statement.setLong(3, leaderLease.toMillis());
            return statement.executeUpdate() == 1;
        }
    }

    public Optional<Claim> claimDue(Connection connection, String owner, Duration lease, int shardIndex, int shardCount) throws SQLException {
        String shardFilter = shardCount == 1 ? "" :
                " AND mod((('x' || left(md5({id}::text), 8))::bit(32)::bigint), " + shardCount + ") = " + shardIndex;
        String sql = """
            WITH candidate AS (
                SELECT {id} FROM {table}
                WHERE {status} IN ('READY','PROCESSING') AND {available_at} <= statement_timestamp()
                %s
                ORDER BY {available_at}, {id} LIMIT 1 FOR UPDATE SKIP LOCKED
            )
            UPDATE {table} j SET
                {status} = CASE WHEN {attempts} = {max_attempts} THEN 'FAILED' ELSE 'PROCESSING' END,
                {attempts} = least({attempts}::bigint + 1, {max_attempts}),
                {owner_id} = CASE WHEN {attempts} < {max_attempts} THEN ? END,
                {claim_token} = CASE WHEN {attempts} < {max_attempts} THEN CAST(? AS uuid) END,
                {available_at} = statement_timestamp() + ? * interval '1 millisecond'
            FROM candidate WHERE j.{id} = candidate.{id}
            RETURNING j.{id}, {payload}, {attempts}, {claim_token}
            """.formatted(shardFilter);
        try (var statement = mapping.prepare(connection, sql)) {
            statement.setString(1, owner);
            statement.setLong(3, lease.toMillis());
            while (true) {
                statement.setObject(2, UUID.randomUUID());
                try (var rows = statement.executeQuery()) {
                    if (!rows.next()) return Optional.empty();
                    UUID token = rows.getObject(4, UUID.class);
                    if (token != null) {
                        return Optional.of(new Claim(rows.getObject(1, UUID.class),
                                rows.getString(2), rows.getInt(3), token));
                    }
                }
            }
        }
    }

    public boolean finish(Connection connection, Claim claim, boolean success, Duration delay) throws SQLException {
        String sql = """
            UPDATE {table} SET
                {status} = CASE WHEN ? THEN 'DONE' WHEN {attempts} >= {max_attempts} THEN 'FAILED' ELSE 'READY' END,
                {available_at} = statement_timestamp() + ? * interval '1 millisecond',
                {owner_id} = NULL, {claim_token} = NULL
            WHERE {id} = ? AND {claim_token} = ? AND {status} = 'PROCESSING'
                AND {available_at} > statement_timestamp()
            """;
        try (var statement = mapping.prepare(connection, sql)) {
            statement.setBoolean(1, success);
            statement.setLong(2, delay.toMillis());
            statement.setObject(3, claim.id());
            statement.setObject(4, claim.token());
            return statement.executeUpdate() == 1;
        }
    }
}
