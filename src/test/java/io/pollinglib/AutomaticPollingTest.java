package io.pollinglib;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import static org.junit.jupiter.api.Assertions.*;

class AutomaticPollingTest {
    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    static class App {}

    static class Handler {
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch invoked = new CountDownLatch(1);

        @KairowPoll(table = "auto_jobs", strategy = KairowPoll.Strategy.MODULO,
                shardIndex = "0", shardCount = "1", pollDelayMillis = 25, jobLeaseMillis = 1500)
        public void handle(PollingQueue.Claim claim) {
            invoked.countDown();
            try {
                if (!release.await(15, TimeUnit.SECONDS)) throw new IllegalStateException("handler timeout");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
    }

    @Test void bootDiscoversAndRunsHandler() throws Exception {
        var db = new PGSimpleDataSource();
        db.setURL(System.getenv().getOrDefault("POLLING_TEST_URL",
                "jdbc:postgresql://localhost:5432/polling?user=postgres&password=postgres"));
        try (var connection = db.getConnection(); var sql = connection.createStatement()) {
            sql.execute("CREATE TABLE auto_jobs (LIKE polling_job INCLUDING ALL)");
            try (var context = new AnnotationConfigApplicationContext()) {
                try (var insert = connection.prepareStatement("INSERT INTO auto_jobs(id,payload,max_attempts) VALUES (?,'work',1)")) {
                    insert.setObject(1, UUID.randomUUID());
                    insert.executeUpdate();
                }
                context.registerBean(DataSource.class, () -> db);
                context.registerBean(Handler.class);
                var registry = new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
                context.registerBean(io.micrometer.core.instrument.MeterRegistry.class, () -> registry);
                context.register(App.class);
                context.refresh();
                var handler = context.getBean(Handler.class);
                assertTrue(handler.invoked.await(10, TimeUnit.SECONDS));
                var draining = java.util.concurrent.CompletableFuture.runAsync(context::stop);
                try {
                    Thread.sleep(1800);
                    assertFalse(draining.isDone(), "shutdown must drain the active handler");
                    try (var rows = sql.executeQuery("SELECT available_at > now() FROM auto_jobs")) {
                        assertTrue(rows.next());
                        assertTrue(rows.getBoolean(1), "job lease must renew during shutdown");
                    }
                } finally {
                    handler.release.countDown();
                }
                draining.get(10, TimeUnit.SECONDS);
                assertEquals(1.0, registry.get("kairow.completed").counter().count());
                assertEquals(1, registry.get("kairow.processing.duration").timer().count());
            } finally {
                sql.execute("DROP TABLE auto_jobs");
            }
        }
    }
}
