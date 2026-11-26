package io.pollinglib;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;

/** JDBC queue shared by workers using the same writable database. */
public final class PollingQueue {
    public record Claim(UUID id, String payload, int attempt, UUID token) {}

    private final DataSource source;

    private final QueueBackend backend;
    private final int shardIndex;
    private final int shardCount;
    private final UUID leaderToken = UUID.randomUUID();
    private final Duration leaderLease;

    public PollingQueue(DataSource source) {
        this(source, QueueBackend.POSTGRESQL);
    }

    public PollingQueue(DataSource source, QueueBackend backend) {
        this(source, backend, 0, 1, null);
    }

    public static PollingQueue singleActive(DataSource source, Duration lease) {
        return singleActive(source, QueueBackend.POSTGRESQL, lease);
    }

    public static PollingQueue singleActive(DataSource source, QueueBackend backend, Duration lease) {
        if (lease.toMillis() < 1) throw new IllegalArgumentException("Positive leader lease required");
        return new PollingQueue(source, backend, 0, 1, lease);
    }

    public static PollingQueue modulo(DataSource source, int index, int count) {
        return modulo(source, QueueBackend.POSTGRESQL, index, count);
    }

    public static PollingQueue modulo(DataSource source, QueueBackend backend, int index, int count) {
        if (count < 1 || index < 0 || index >= count) throw new IllegalArgumentException("Invalid shard");
        return new PollingQueue(source, backend, index, count, null);
    }

    private PollingQueue(DataSource source, QueueBackend backend, int index, int count, Duration lease) {
        this.source = Objects.requireNonNull(source);
        this.backend = Objects.requireNonNull(backend);
        this.shardIndex = index;
        this.shardCount = count;
        this.leaderLease = lease;
    }

    public void enqueue(Connection tx, UUID id, String payload, int maxAttempts) throws SQLException {
        backend.enqueue(tx, id, payload, maxAttempts);
    }

    private Connection open() throws SQLException {
        var connection = source.getConnection();
        try {
            connection.setAutoCommit(true);
            return connection;
        } catch (SQLException e) {
            connection.close();
            throw e;
        }
    }

    /** Claims one due job. Also retires expired final attempts before returning. */
    public Optional<Claim> claim(String owner, Duration lease) throws SQLException {
        if (owner == null || owner.isBlank() || lease.toMillis() < 1) {
            throw new IllegalArgumentException("Supply an owner and a positive lease of at least 1 ms");
        }
        try (var connection = open()) {
            connection.setAutoCommit(false);
            try {
                var result = leaderLease == null || backend.acquireLeadership(connection, leaderToken, leaderLease)
                        ? backend.claimDue(connection, owner, lease, shardIndex, shardCount) : Optional.<Claim>empty();
                connection.commit();
                return result;
            } catch (SQLException | RuntimeException failure) {
                connection.rollback();
                throw failure;
            }
        }
    }

    public boolean complete(Claim claim) throws SQLException {
        return finish(claim, true, Duration.ZERO);
    }

    public boolean retry(Claim claim, Duration delay) throws SQLException {
        if (delay.isNegative()) throw new IllegalArgumentException("Negative retry delay");
        return finish(claim, false, delay);
    }

    private boolean finish(Claim claim, boolean success, Duration delay) throws SQLException {
        try (var connection = open()) {
            return backend.finish(connection, claim, success, delay);
        }
    }
}
