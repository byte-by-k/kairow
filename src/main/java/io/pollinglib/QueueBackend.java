package io.pollinglib;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import io.pollinglib.PollingQueue.Claim;

/** Stateless database operations. Implementations leave connection and transaction ownership to the caller. */
public interface QueueBackend {
    QueueBackend POSTGRESQL = new PostgresBackend();
    QueueBackend MYSQL = new MysqlBackend();

    void enqueue(Connection tx, UUID id, String payload, int maxAttempts) throws SQLException;
    /** Serialize the leadership decision and subsequent claim in the supplied transaction. */
    boolean acquireLeadership(Connection tx, UUID token, Duration lease) throws SQLException;
    /** Atomically lock and claim a due row, recovering expired attempts in the same transaction. */
    Optional<Claim> claimDue(Connection tx, String owner, Duration lease, int shardIndex, int shardCount) throws SQLException;
    /** Guard completion or retry by the current token and expiry in one statement. */
    boolean finish(Connection tx, Claim claim, boolean success, Duration delay) throws SQLException;
}
