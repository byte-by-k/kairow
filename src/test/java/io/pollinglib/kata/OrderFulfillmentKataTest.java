package io.pollinglib.kata;

import io.pollinglib.AnnotatedPoller;
import io.pollinglib.KairowPoll;
import io.pollinglib.PollingQueue;
import io.pollinglib.QueueBackend;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** Runnable reference solution; see examples/order-fulfillment-kata/README.md. */
public class OrderFulfillmentKataTest {
    private final PGSimpleDataSource db = new PGSimpleDataSource();
    private final String schema = "kata_" + UUID.randomUUID().toString().replace("-", "");
    private boolean schemaCreated;
    private FulfillmentHandler handler;
    private AnnotatedPoller poller;

    @BeforeEach
    void prepare() throws Exception {
        db.setURL(System.getenv().getOrDefault("POLLING_TEST_URL",
                "jdbc:postgresql://localhost:5432/polling?user=postgres&password=postgres"));
        try (var c = db.getConnection(); var s = c.createStatement()) {
            s.execute("CREATE SCHEMA " + schema);
            schemaCreated = true;
        }
        db.setCurrentSchema(schema);
        try (var c = db.getConnection(); var s = c.createStatement()) {
            s.execute(resource("/io/pollinglib/schema.sql"));
            s.execute("CREATE TABLE outbox(id uuid PRIMARY KEY, payload text NOT NULL, "
                    + "aggregate_id text NOT NULL, event_version bigint NOT NULL, "
                    + "UNIQUE(aggregate_id, event_version))");
            s.execute(resource("/io/pollinglib/examples/postgres-companion.sql"));
        }
        handler = new FulfillmentHandler();
        poller = new AnnotatedPoller(handler, "fulfill",
                mapping -> new PollingQueue(db, QueueBackend.postgresql(mapping)));
    }

    @AfterEach
    void cleanup() throws Exception {
        // Only drop the randomly named schema owned by this test.
        if (schemaCreated) {
            try (var c = db.getConnection(); var s = c.createStatement()) {
                s.execute("DROP SCHEMA " + schema + " CASCADE");
            }
        }
    }

    @Test
    void retryBlocksOneOrderButOtherOrdersContinue() throws Exception {
        publish("A", 1, "A:reserve");
        assertThrows(IllegalStateException.class, this::tick);
        publish("A", 2, "A:ship");
        publish("B", 1, "B:reserve");

        assertTrue(tick());
        assertEquals(List.of("B:reserve"), handler.delivered);
        assertFalse(tick(), "A:ship must wait while A:reserve is delayed");

        // Advance the retry in the fixture instead of sleeping for an hour.
        try (var c = db.getConnection(); var s = c.createStatement()) {
            assertEquals(1, s.executeUpdate("UPDATE outbox_state SET available_at=now() "
                    + "WHERE aggregate_id='A' AND event_version=1 AND status='READY'"));
        }
        assertTrue(tick());
        assertTrue(tick());
        assertFalse(tick());
        assertEquals(List.of("B:reserve", "A:reserve", "A:ship"), handler.delivered);
        assertEquals(List.of(1, 2, 1), handler.attempts);
        assertEquals(3, count("outbox_state WHERE status='DONE'"));
        assertEquals(3, count("outbox"), "Companion polling preserves source events");
    }

    @Test
    void terminalFailureBlocksLaterEventsOfThatOrder() throws Exception {
        publish("A", 1, "invalid");
        assertThrows(IllegalArgumentException.class, this::tick);
        publish("A", 2, "A:ship");
        publish("B", 1, "B:reserve");
        assertTrue(tick());
        assertFalse(tick());
        assertEquals(List.of("B:reserve"), handler.delivered);
        assertEquals(1, count("outbox_state WHERE status='FAILED'"));
        assertEquals(1, count("outbox_state WHERE status='READY'"));
    }

    @Test
    void rolledBackProducerTransactionPublishesNothing() throws Exception {
        try (var c = db.getConnection()) {
            c.setAutoCommit(false);
            insert(c, "A", 1, "A:reserve");
            c.rollback();
        }
        assertEquals(0, count("outbox"));
        assertEquals(0, count("outbox_state"));
        assertFalse(tick());
    }

    private boolean tick() throws Exception {
        // Manual polling does not start a scheduler or heartbeat thread.
        return poller.pollOnce("kata-worker", Duration.ofMinutes(1), Duration.ofHours(1));
    }

    private void publish(String order, int version, String payload) throws Exception {
        try (var c = db.getConnection()) {
            c.setAutoCommit(false);
            insert(c, order, version, payload);
            c.commit();
        }
    }

    private void insert(Connection c, String order, int version, String payload) throws Exception {
        try (var s = c.prepareStatement("INSERT INTO outbox VALUES (?, ?, ?, ?)")) {
            s.setObject(1, UUID.randomUUID());
            s.setString(2, payload);
            s.setString(3, order);
            s.setInt(4, version);
            s.executeUpdate();
        }
    }

    private int count(String tableAndPredicate) throws Exception {
        try (var c = db.getConnection(); var s = c.createStatement();
             var rows = s.executeQuery("SELECT count(*) FROM " + tableAndPredicate)) {
            rows.next();
            return rows.getInt(1);
        }
    }

    private String resource(String path) throws Exception {
        try (var stream = getClass().getResourceAsStream(path)) {
            if (stream == null) throw new IllegalStateException("Missing resource: " + path);
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    public static class FulfillmentHandler {
        final List<String> delivered = new ArrayList<>();
        final List<Integer> attempts = new ArrayList<>();

        @KairowPoll(table = "outbox", companionStateTable = "outbox_state",
                aggregateKeyColumn = "aggregate_id", aggregateSequenceColumn = "event_version",
                nonRetryable = IllegalArgumentException.class)
        public void fulfill(PollingQueue.Claim event) {
            if (event.payload().equals("invalid")) {
                throw new IllegalArgumentException("Unknown fulfillment command");
            }
            if (event.payload().equals("A:reserve") && event.attempt() == 1) {
                throw new IllegalStateException("Inventory service temporarily unavailable");
            }
            delivered.add(event.payload());
            attempts.add(event.attempt());
        }
    }
}
