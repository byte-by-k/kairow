package io.pollinglib;

import com.mysql.cj.jdbc.MysqlDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Duration;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class OrderedBatchTest {
    @KairowPoll(table="outbox", companionStateTable="outbox_state", aggregateKeyColumn="aggregate_id",
            aggregateSequenceColumn="event_version", retryBackoff=KairowPoll.Backoff.EXPONENTIAL,
            maxRetryDelayMillis=500, nonRetryable=IllegalArgumentException.class)
    public void handle(PollingQueue.Claim claim) { throw new IllegalArgumentException(); }

    private KairowPoll options() throws Exception {
        return getClass().getMethod("handle", PollingQueue.Claim.class).getAnnotation(KairowPoll.class);
    }

    @Test void exponentialDelayCapsWithoutOverflow() throws Exception {
        assertEquals(100, AnnotatedPoller.retryDelay(options(), 1, Duration.ofMillis(100)).toMillis());
        assertEquals(200, AnnotatedPoller.retryDelay(options(), 2, Duration.ofMillis(100)).toMillis());
        assertEquals(500, AnnotatedPoller.retryDelay(options(), Integer.MAX_VALUE, Duration.ofMillis(100)).toMillis());
    }

    @Test void postgresOrderedCompanion() throws Exception {
        var db = new PGSimpleDataSource();
        db.setURL(System.getenv().getOrDefault("POLLING_TEST_URL",
                "jdbc:postgresql://localhost:5432/polling?user=postgres&password=postgres"));
        check(db, false);
    }

    @Test @EnabledIfEnvironmentVariable(named="KAIROW_MYSQL_TEST", matches="true")
    void mysqlOrderedCompanion() throws Exception {
        var db = new MysqlDataSource();
        db.setURL("jdbc:mysql://127.0.0.1:3306/polling?allowPublicKeyRetrieval=true&useSSL=false");
        db.setUser("kairow");
        db.setPassword("kairow");
        check(db, true);
    }

    private void check(DataSource db, boolean mysql) throws Exception {
        var m = QueueMapping.from(options());
        var backend = mysql ? QueueBackend.mysql(m) : QueueBackend.postgresql(m);
        var queue = new PollingQueue(db, backend);
        var minute = Duration.ofMinutes(1);
        var first = UUID.randomUUID();
        var later = UUID.randomUUID();
        try (var tx = db.getConnection(); var sql = tx.createStatement()) {
            sql.execute("CREATE TABLE outbox(id "+(mysql ? "char(36)" : "uuid")+
                    " PRIMARY KEY, payload text, aggregate_id varchar(128), event_version bigint)");
            insert(tx, mysql, first, "A", 1);
            String path = "/io/pollinglib/examples/"+(mysql ? "mysql" : "postgres")+"-companion.sql";
            try (var stream = getClass().getResourceAsStream(path)) {
                String migration = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
                if (mysql) {
                    for (String command : migration.replaceAll("(?m)^--.*$", "").split(";"))
                        if (!command.isBlank()) sql.execute(command);
                } else sql.execute(migration);
            }
            insert(tx, mysql, later, "A", 2);
            insert(tx, mysql, UUID.randomUUID(), "B", 1);
            tx.setAutoCommit(false);
            var batch = backend.claimBatch(tx, "pod1", minute, 0, 1, 8);
            assertEquals(2, batch.size());
            assertTrue(queue.claim("pod2", minute).isEmpty());
            tx.commit();
            tx.setAutoCommit(true);
            for (var claim : batch) {
                if (claim.id().equals(first)) {
                    assertTrue(queue.renew(claim, Duration.ofMinutes(2)));
                    assertTrue(queue.retry(claim, Duration.ZERO));
                    assertFalse(queue.renew(claim, minute));
                } else assertTrue(queue.complete(claim));
            }
            var retry = queue.claim("pod2", minute).orElseThrow();
            assertEquals(first, retry.id());
            assertEquals(2, retry.attempt());
            assertTrue(queue.complete(retry));
            var last = queue.claim("pod2", minute).orElseThrow();
            assertEquals(later,last.id());
            assertTrue(queue.retry(last, Duration.ZERO));
            var poller = new AnnotatedPoller(this,"handle", ignored -> queue);
            assertThrows(IllegalArgumentException.class, () -> poller.pollOnce("pod2",minute,Duration.ZERO));
            assertTrue(queue.claim("pod2",minute).isEmpty());
            try (var rows = sql.executeQuery("SELECT COUNT(*) FROM outbox_state WHERE status='FAILED'")) {
                assertTrue(rows.next());
                assertEquals(1,rows.getInt(1));
            }
        } finally {
            try (var c=db.getConnection(); var sql=c.createStatement()) {
                sql.execute("DROP TABLE IF EXISTS outbox");
                sql.execute("DROP TABLE IF EXISTS outbox_state");
                if(mysql) sql.execute("DELETE FROM polling_leader WHERE queue_name='outbox_state'");
                else sql.execute("DROP FUNCTION IF EXISTS kairow_capture_outbox()");
            }
        }
    }

    private void insert(Connection c, boolean mysql, UUID id, String key, int seq) throws SQLException {
        try (var s=c.prepareStatement("INSERT INTO outbox VALUES (?,'data',?,?)")) {
            s.setObject(1,mysql ? id.toString() : id);
            s.setString(2,key);
            s.setInt(3,seq);
            s.executeUpdate();
        }
    }
}
