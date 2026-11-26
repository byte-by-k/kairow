package io.pollinglib;

import com.mysql.cj.jdbc.MysqlDataSource;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named = "KAIROW_MYSQL_TEST", matches = "true")
class MysqlQueueTest {
    @Test void leaderExclusionAndShardedRetry() throws Exception {
        var db = new MysqlDataSource();
        db.setURL("jdbc:mysql://127.0.0.1:3306/polling?allowPublicKeyRetrieval=true&useSSL=false");
        db.setUser("kairow");
        db.setPassword("kairow");
        var lease = Duration.ofMinutes(1);
        var leader = PollingQueue.singleActive(db, QueueBackend.MYSQL, lease);
        var standby = PollingQueue.singleActive(db, QueueBackend.MYSQL, lease);
        var id = UUID.randomUUID();
        try (var tx = db.getConnection()) {
            leader.enqueue(tx, id, "work", 3);
        }
        var claim = leader.claim("a", lease).orElseThrow();
        assertTrue(leader.retry(claim, Duration.ZERO));
        assertTrue(standby.claim("b", lease).isEmpty());
        var even = PollingQueue.modulo(db, QueueBackend.MYSQL, 0, 2);
        var odd = PollingQueue.modulo(db, QueueBackend.MYSQL, 1, 2);
        var retry = even.claim("even", lease);
        var other = odd.claim("odd", lease);
        assertNotEquals(retry.isPresent(), other.isPresent());
        var active = retry.isPresent() ? retry.get() : other.orElseThrow();
        assertEquals(2, active.attempt());
        assertFalse(leader.complete(claim));
        assertTrue(leader.complete(active));
    }
}
