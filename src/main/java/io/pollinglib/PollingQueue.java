package io.pollinglib;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;

/** PostgreSQL queue shared by workers using the same writable database. */
public final class PollingQueue {
    public record Claim(UUID id, String payload, int attempt, UUID token) {}

    private final DataSource source;

    public PollingQueue(DataSource source) {
        this.source = Objects.requireNonNull(source);
    }

    /** Uses the caller's transaction, so enqueue can commit with business data. */
    public void enqueue(Connection tx, UUID id, String payload, int maxAttempts) throws SQLException {
        try (var statement = tx.prepareStatement(
                "INSERT INTO polling_job (id, payload, max_attempts) VALUES (?, ?, ?)")) {
            statement.setObject(1, id);
            statement.setString(2, payload);
            statement.setInt(3, maxAttempts);
            statement.executeUpdate();
        }
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
        String sql = """
            WITH candidate AS (
                SELECT id FROM polling_job
                WHERE status IN ('READY','PROCESSING') AND available_at <= statement_timestamp()
                ORDER BY available_at, id LIMIT 1 FOR UPDATE SKIP LOCKED
            )
            UPDATE polling_job j SET
                status = CASE WHEN attempts = max_attempts THEN 'FAILED' ELSE 'PROCESSING' END,
                attempts = least(attempts::bigint + 1, max_attempts),
                owner_id = CASE WHEN attempts < max_attempts THEN ? END,
                claim_token = CASE WHEN attempts < max_attempts THEN CAST(? AS uuid) END,
                available_at = statement_timestamp() + ? * interval '1 millisecond'
            FROM candidate WHERE j.id = candidate.id
            RETURNING j.id, payload, attempts, claim_token
            """;
        try (var connection = open(); var statement = connection.prepareStatement(sql)) {
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

    public boolean complete(Claim claim) throws SQLException {
        return finish(claim, true, Duration.ZERO);
    }

    public boolean retry(Claim claim, Duration delay) throws SQLException {
        if (delay.isNegative()) throw new IllegalArgumentException("Negative retry delay");
        return finish(claim, false, delay);
    }

    private boolean finish(Claim claim, boolean success, Duration delay) throws SQLException {
        String sql = """
            UPDATE polling_job SET
                status = CASE WHEN ? THEN 'DONE' WHEN attempts >= max_attempts THEN 'FAILED' ELSE 'READY' END,
                available_at = statement_timestamp() + ? * interval '1 millisecond',
                owner_id = NULL, claim_token = NULL
            WHERE id = ? AND claim_token = ? AND status = 'PROCESSING'
                AND available_at > statement_timestamp()
            """;
        try (var connection = open(); var statement = connection.prepareStatement(sql)) {
            statement.setBoolean(1, success);
            statement.setLong(2, delay.toMillis());
            statement.setObject(3, claim.id());
            statement.setObject(4, claim.token());
            return statement.executeUpdate() == 1;
        }
    }
}
