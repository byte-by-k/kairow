package io.pollinglib;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import io.pollinglib.PollingQueue.Claim;

final class MysqlBackend extends JdbcClaimBackend {
    MysqlBackend(QueueMapping mapping) { super(mapping, true); }

    public void enqueue(Connection tx, UUID id, String payload, int maxAttempts) throws SQLException {
        try (var s = mapping.prepare(tx, "INSERT INTO {table}({id},{payload},{max_attempts}) VALUES (?,?,?)")) {
            s.setString(1, id.toString());
            s.setString(2, payload);
            s.setInt(3, maxAttempts);
            s.executeUpdate();
        }
    }

    public boolean acquireLeadership(Connection tx, UUID token, Duration lease) throws SQLException {
        // Migration seeds the singleton. Lock it before checking time so waits cannot stale the decision.
        try (var s = mapping.prepare(tx, "SELECT queue_name FROM {leader} WHERE queue_name='{table}' FOR UPDATE");
             var rows = s.executeQuery()) {
            if (!rows.next()) throw new SQLException("Missing polling_job leadership row");
        }
        try (var s = mapping.prepare(tx, """
                UPDATE {leader} SET token=?, expires_at=TIMESTAMPADD(MICROSECOND, ?, CURRENT_TIMESTAMP(6))
                WHERE queue_name='{table}' AND (token=? OR expires_at IS NULL OR expires_at<=CURRENT_TIMESTAMP(6))
                """)) {
            s.setString(1, token.toString());
            s.setLong(2, micros(lease));
            s.setString(3, token.toString());
            return s.executeUpdate() == 1;
        }
    }

    public boolean finish(Connection tx, Claim claim, boolean success, Duration delay) throws SQLException {
        try (var s = mapping.prepare(tx, """
                UPDATE {table} SET {status}=CASE WHEN ? THEN 'DONE' WHEN {attempts}>={max_attempts} THEN 'FAILED' ELSE 'READY' END,
                {available_at}=TIMESTAMPADD(MICROSECOND, ?, CURRENT_TIMESTAMP(6)),{owner_id}=NULL,{claim_token}=NULL
                WHERE {id}=? AND {claim_token}=? AND {status}='PROCESSING' AND {available_at}>CURRENT_TIMESTAMP(6)
                """)) {
            s.setBoolean(1, success);
            s.setLong(2, micros(delay));
            s.setString(3, claim.id().toString());
            s.setString(4, claim.token().toString());
            return s.executeUpdate() == 1;
        }
    }

    private static long micros(Duration duration) {
        return Math.multiplyExact(duration.toMillis(), 1000L);
    }
}
