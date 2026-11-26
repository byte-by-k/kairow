package io.pollinglib;

import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import static org.junit.jupiter.api.Assertions.*;

class PollingQueueTest {
    static final class OutboxHandler {
        UUID processed;

        @KairowPoll(table = "test_events", idColumn = "event_id", payloadColumn = "body")
        public void publish(PollingQueue.Claim claim) {
            processed = claim.id();
        }
    }

    @Test void annotatedPollerClaimsAndCompletesMappedRow() throws Exception {
        var db = database();
        var handler = new OutboxHandler();
        var poller = new AnnotatedPoller(handler, "publish", mapping ->
                new PollingQueue(db, QueueBackend.postgresql(mapping)));
        var id = UUID.randomUUID();
        try (var tx = db.getConnection(); var sql = tx.createStatement()) {
            sql.execute("CREATE TABLE test_events (LIKE polling_job INCLUDING ALL)");
            try {
                sql.execute("ALTER TABLE test_events RENAME COLUMN id TO event_id");
                sql.execute("ALTER TABLE test_events RENAME COLUMN payload TO body");
                try (var insert = tx.prepareStatement("INSERT INTO test_events(event_id,body,max_attempts) VALUES (?,'event',2)")) {
                    insert.setObject(1, id);
                    insert.executeUpdate();
                }
                assertTrue(poller.pollOnce("pod", Duration.ofMinutes(1), Duration.ZERO));
                assertEquals(id, handler.processed);
                assertFalse(poller.pollOnce("pod", Duration.ofMinutes(1), Duration.ZERO));
                try (var rows = sql.executeQuery("SELECT status FROM test_events")) {
                    assertTrue(rows.next());
                    assertEquals("DONE", rows.getString(1));
                }
            } finally {
                sql.execute("DROP TABLE test_events");
            }
        }
    }

    private PGSimpleDataSource database() {
        var db = new PGSimpleDataSource();
        db.setURL(System.getenv().getOrDefault("POLLING_TEST_URL",
                "jdbc:postgresql://localhost:5432/polling?user=postgres&password=postgres"));
        return db;
    }

    @Test void idleLeaderKeepsStandbyFromClaimingUntilExpiry() throws Exception {
        var db = database();
        var lease = Duration.ofMinutes(1);
        var first = PollingQueue.singleActive(db, lease);
        var standby = PollingQueue.singleActive(db, lease);
        try (var tx = db.getConnection(); var sql = tx.createStatement()) {
            sql.executeUpdate("DELETE FROM polling_leader WHERE queue_name='polling_job'");
            assertTrue(first.claim("pod", lease).isEmpty());
            var id = UUID.randomUUID();
            first.enqueue(tx, id, "waiting", 2);
            assertTrue(standby.claim("pod", lease).isEmpty());
            sql.executeUpdate("UPDATE polling_leader SET expires_at=now() WHERE queue_name='polling_job'");
            var claim = standby.claim("pod", lease).orElseThrow();
            assertEquals(id, claim.id());
            assertTrue(standby.complete(claim));
        }
    }

    @Test void moduloRoutesExistingRowsAndRetriesToTheirShard() throws Exception {
        var db = database();
        var shared = new PollingQueue(db);
        var even = PollingQueue.modulo(db, 0, 2);
        var odd = PollingQueue.modulo(db, 1, 2);
        var lease = Duration.ofMinutes(1);
        assertThrows(IllegalArgumentException.class, () -> PollingQueue.modulo(db, 2, 2));
        var evenId = UUID.fromString("00000000-0000-0000-0000-000000000000");
        var oddId = UUID.fromString("00000000-0000-0000-0000-000000000005");
        try (var tx = db.getConnection()) {
            shared.enqueue(tx, evenId, "even", 2);
            shared.enqueue(tx, oddId, "odd", 2);
        }
        var first = even.claim("a", lease).orElseThrow();
        assertEquals(evenId, first.id());
        assertTrue(even.claim("a", lease).isEmpty());
        var second = odd.claim("b", lease).orElseThrow();
        assertEquals(oddId, second.id());
        assertTrue(odd.complete(second));
        assertTrue(even.retry(first, Duration.ZERO));
        assertTrue(odd.claim("b", lease).isEmpty());
        var retry = even.claim("replacement", lease).orElseThrow();
        assertEquals(evenId, retry.id());
        assertEquals(2, retry.attempt());
        assertTrue(even.complete(retry));
    }

    @Test void lockedRowsRecoveryAndRetryLimit() throws Exception {
        var db = database();
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
