package io.pollinglib;

import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import static org.junit.jupiter.api.Assertions.*;

class PollingQueueTest {
    @Test void lockedRowsRecoveryAndRetryLimit() throws Exception {
        var db = new PGSimpleDataSource();
        db.setURL(System.getenv().getOrDefault("POLLING_TEST_URL",
                "jdbc:postgresql://localhost:5432/polling?user=postgres&password=postgres"));
        var queue = new PollingQueue(db);
        var id = UUID.randomUUID();
        var lease = Duration.ofMinutes(2);
        try (var tx = db.getConnection(); var sql = tx.createStatement()) {
            tx.setAutoCommit(false);
            queue.enqueue(tx, id, "example", 2);
            assertTrue(queue.claim("other", lease).isEmpty());
            tx.commit();
            sql.execute("SELECT id FROM polling_job WHERE status='READY' FOR UPDATE");
            assertTrue(queue.claim("other", lease).isEmpty());
            tx.commit();
            var first = queue.claim("a", lease).orElseThrow();
            assertEquals(id, first.id());
            assertTrue(queue.claim("b", lease).isEmpty());
            try (var expire = tx.prepareStatement("UPDATE polling_job SET available_at=now() WHERE id=?")) {
                expire.setObject(1, id);
                expire.executeUpdate();
            }
            tx.commit();
            var second = queue.claim("b", lease).orElseThrow();
            assertFalse(queue.complete(first));
            assertEquals(2, second.attempt());
            assertTrue(queue.retry(second, Duration.ZERO));
            assertTrue(queue.claim("c", lease).isEmpty());
            try (var result = sql.executeQuery("SELECT status FROM polling_job WHERE id='" + id + "'")) {
                assertTrue(result.next());
                assertEquals("FAILED", result.getString(1));
            }
        }
    }
}
