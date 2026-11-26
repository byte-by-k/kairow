package io.pollinglib;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import io.pollinglib.PollingQueue.Claim;

/** Stateless database operations. Implementations leave connection and transaction ownership to the caller. */
public interface QueueBackend {
    QueueBackend POSTGRESQL = postgresql(QueueMapping.DEFAULT);
    QueueBackend MYSQL = mysql(QueueMapping.DEFAULT);

    static QueueBackend postgresql(QueueMapping mapping) { return new PostgresBackend(mapping); }
    static QueueBackend mysql(QueueMapping mapping) { return new MysqlBackend(mapping); }

    void enqueue(Connection tx, UUID id, String payload, int maxAttempts) throws SQLException;
    /** Serialize the leadership decision and subsequent claim in the supplied transaction. */
    boolean acquireLeadership(Connection tx, UUID token, Duration lease) throws SQLException;
    /** Atomically lock and claim a due row, recovering expired attempts in the same transaction. */
    Optional<Claim> claimDue(Connection tx, String owner, Duration lease, int shardIndex, int shardCount) throws SQLException;
    default java.util.List<Claim> claimBatch(Connection tx, String owner, Duration lease, int shard, int count, int size) throws SQLException {
        if (size != 1) throw new UnsupportedOperationException("Backend does not support batch claiming");
        return claimDue(tx, owner, lease, shard, count).stream().toList();
    }
    default boolean renew(Connection tx, Claim claim, Duration lease) throws SQLException {
        throw new UnsupportedOperationException("Backend does not support lease renewal");
    }
    default boolean fail(Connection tx, Claim claim) throws SQLException {
        throw new UnsupportedOperationException("Backend does not support terminal failure");
    }
    /** Guard completion or retry by the current token and expiry in one statement. */
    boolean finish(Connection tx, Claim claim, boolean success, Duration delay) throws SQLException;
}
