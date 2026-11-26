package io.pollinglib;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import io.pollinglib.PollingQueue.Claim;

final class MysqlBackend implements QueueBackend {
    public void enqueue(Connection tx, UUID id, String payload, int maxAttempts) throws SQLException {
        try (var s = tx.prepareStatement("INSERT INTO polling_job(id,payload,max_attempts) VALUES (?,?,?)")) {
            s.setString(1, id.toString());
            s.setString(2, payload);
            s.setInt(3, maxAttempts);
            s.executeUpdate();
        }
    }

    public boolean acquireLeadership(Connection tx, UUID token, Duration lease) throws SQLException {
        // Migration seeds the singleton. Lock it before checking time so waits cannot stale the decision.
        try (var s = tx.prepareStatement("SELECT queue_name FROM polling_leader WHERE queue_name='polling_job' FOR UPDATE");
             var rows = s.executeQuery()) {
            if (!rows.next()) throw new SQLException("Missing polling_job leadership row");
        }
        try (var s = tx.prepareStatement("""
                UPDATE polling_leader SET token=?, expires_at=TIMESTAMPADD(MICROSECOND, ?, CURRENT_TIMESTAMP(6))
                WHERE queue_name='polling_job' AND (token=? OR expires_at IS NULL OR expires_at<=CURRENT_TIMESTAMP(6))
                """)) {
            s.setString(1, token.toString());
            s.setLong(2, micros(lease));
            s.setString(3, token.toString());
            return s.executeUpdate() == 1;
        }
    }

    public Optional<Claim> claimDue(Connection tx, String owner, Duration lease, int index, int count) throws SQLException {
        String shard = count == 1 ? "" : " AND MOD(CAST(CONV(LEFT(MD5(id),8),16,10) AS UNSIGNED), ?) = ?";
        String sql = "SELECT id,payload,attempts,max_attempts FROM polling_job WHERE status IN ('READY','PROCESSING')"
                + " AND available_at<=CURRENT_TIMESTAMP(6)" + shard + " ORDER BY available_at,id LIMIT 1 FOR UPDATE SKIP LOCKED";
        try (var select = tx.prepareStatement(sql)) {
            if (count > 1) {
                select.setInt(1, count);
                select.setInt(2, index);
            }
            while (true) {
                try (var rows = select.executeQuery()) {
                    if (!rows.next()) return Optional.empty();
                    UUID id = UUID.fromString(rows.getString(1));
                    if (rows.getInt(3) >= rows.getInt(4)) {
                        try (var retire = tx.prepareStatement("UPDATE polling_job SET status='FAILED',claim_token=NULL,owner_id=NULL WHERE id=?")) {
                            retire.setString(1, id.toString());
                            retire.executeUpdate();
                        }
                        continue;
                    }
                    var claim = new Claim(id, rows.getString(2), rows.getInt(3) + 1, UUID.randomUUID());
                    try (var update = tx.prepareStatement("""
                            UPDATE polling_job SET status='PROCESSING',attempts=attempts+1,owner_id=?,claim_token=?,
                            available_at=TIMESTAMPADD(MICROSECOND, ?, CURRENT_TIMESTAMP(6)) WHERE id=?
                            """)) {
                        update.setString(1, owner);
                        update.setString(2, claim.token().toString());
                        update.setLong(3, micros(lease));
                        update.setString(4, id.toString());
                        update.executeUpdate();
                    }
                    return Optional.of(claim);
                }
            }
        }
    }

    public boolean finish(Connection tx, Claim claim, boolean success, Duration delay) throws SQLException {
        try (var s = tx.prepareStatement("""
                UPDATE polling_job SET status=CASE WHEN ? THEN 'DONE' WHEN attempts>=max_attempts THEN 'FAILED' ELSE 'READY' END,
                available_at=TIMESTAMPADD(MICROSECOND, ?, CURRENT_TIMESTAMP(6)),owner_id=NULL,claim_token=NULL
                WHERE id=? AND claim_token=? AND status='PROCESSING' AND available_at>CURRENT_TIMESTAMP(6)
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
