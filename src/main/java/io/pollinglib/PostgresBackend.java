package io.pollinglib;

import java.sql.*;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import io.pollinglib.PollingQueue.Claim;

final class PostgresBackend extends JdbcClaimBackend {
    PostgresBackend(QueueMapping mapping) { super(mapping, false); }

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
