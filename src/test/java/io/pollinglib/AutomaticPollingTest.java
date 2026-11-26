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
        final CountDownLatch invoked = new CountDownLatch(1);

        @KairowPoll(table = "auto_jobs", strategy = KairowPoll.Strategy.MODULO,
                shardIndex = "0", shardCount = "1", pollDelayMillis = 25)
        public void handle(PollingQueue.Claim claim) {
            invoked.countDown();
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
                context.register(App.class);
                context.refresh();
                assertTrue(context.getBean(Handler.class).invoked.await(10, TimeUnit.SECONDS));
            } finally {
                sql.execute("DROP TABLE auto_jobs");
            }
        }
    }
}
