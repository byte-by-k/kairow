# Kairow

Kairow is a Java 17 polling SDK for PostgreSQL and MySQL. In Spring Boot 3, annotate a handler to claim rows in batches, process different aggregates concurrently, renew job leases, and publish Micrometer metrics. The host supplies the JDBC driver and database migrations.

## Why Kairow?

Kairow (pronounced *KAI-roh*) blends the Greek word *kairos*, meaning “the right moment,” with *row*, the PostgreSQL record that holds a job. A worker can claim that row when a new job is ready, a retry becomes due, or a previous worker’s lease expires. The name captures what the framework helps a worker find: the right row at the right moment.

## Usage

Choose a backend for the database behind the DataSource. Existing constructors default to PostgreSQL.

| Database | Backend | Initial schema under src/main/resources/io/pollinglib |
|---|---|---|
| PostgreSQL | `QueueBackend.POSTGRESQL` | `schema.sql` (existing installations keep their tables) |
| MySQL 8.0.16+ with InnoDB | `QueueBackend.MYSQL` | `mysql.sql` (includes the singleton leadership row) |

```java
var shared = new PollingQueue(dataSource, QueueBackend.MYSQL);
var sharded = PollingQueue.modulo(dataSource, QueueBackend.MYSQL, shardIndex, shardCount);
var leader = PollingQueue.singleActive(dataSource, QueueBackend.MYSQL, Duration.ofSeconds(30));
```

Apply the matching schema once. Use a consistent UTC session time zone on all MySQL connections and READ COMMITTED for the polling pool to reduce range locking. Provide the vendor driver in your application. For a different relational database, implement `QueueBackend` and provide its schema. The backend owns vendor SQL, atomic claiming, leadership and token-guarded updates. All pods working on the same queue must use the same database and backend.
 Call enqueue with a stable UUID and the same JDBC connection as your business transaction. Duplicate IDs raise a primary key violation.

A worker tick can process one job as follows:

```java
var next = queue.claim(podName, Duration.ofMinutes(2));
if (next.isEmpty()) return;
var job = next.get();
try {
    handler.process(job.id(), job.payload());
} catch (Exception failure) {
    logger.warn("Processing failed for {}", job.id(), failure);
    if (!queue.retry(job, Duration.ofSeconds(30))) {
        logger.warn("Lost claim for {}", job.id());
    }
    return;
}
if (!queue.complete(job)) logger.warn("Lost claim for {}", job.id());
```

Manual JDBC workers can renew a live claim with `queue.renew(claim, lease)`. Size the lease for the longest gap between renewals. A crash or lease expiry permits redelivery. The unique claim token fences queue updates. Make downstream effects idempotent using the job ID. All pods must use one authoritative writable database. Independently writable databases require external coordination for split-brain protection.

## Polling across instances

The default shared queue lets every worker claim any due row using `SKIP LOCKED`. This is the simplest starting point for autoscaling and uneven job durations.

For static modulo sharding, construct each worker with its zero-based shard index and a common shard count:

```java
var queue = PollingQueue.modulo(dataSource, shardIndex, shardCount);
```

With four shards, deploy workers for indexes 0 through 3. Each job maps to one shard using the unsigned first 32 bits of MD5 of its canonical UUID text, modulo four. Existing jobs and retries use the same mapping without migration. Multiple replicas may serve a shard safely. A shard without a live worker waits until a replacement starts or a shared-queue worker picks up its jobs. Sharding provides no FIFO guarantee.

Use a stable assignment such as a StatefulSet ordinal. When changing the count, maintain coverage for every shard in the new layout before retiring the old workers. Overlapping layouts still coordinate through PostgreSQL row locks and leases.

For a large queue, benchmark the shared queue against modulo with the same database, backlog and handler concurrency. Compare completed jobs per second, oldest due job per shard, claim latency and database CPU. Without a matching index, each shard can scan rows belonging to other shards. For a fixed four-shard layout, an optional PostgreSQL index is:

```sql
CREATE INDEX CONCURRENTLY polling_job_due_mod4 ON polling_job
    (mod((('x' || left(md5(id::text), 8))::bit(32)::bigint), 4), available_at, id)
    WHERE status IN ('READY','PROCESSING');
```

## Single active poller

Use one long-lived queue object per pod, and configure every pod for the same mode:

```java
var queue = PollingQueue.singleActive(dataSource, Duration.ofSeconds(30));
```

On each scheduled claim, the active pod renews its leadership in the database, even when the queue is empty. Standbys check the leadership row and return an empty result without reading jobs. Renewal and the job claim share a transaction. If the leader stops renewing, another pod can take over on its next tick after expiry. Existing databases must apply the polling_leader CREATE TABLE statement at the end of schema.sql before enabling this mode.

Choose a leadership lease longer than the largest expected gap between claim calls, including handler time for a synchronous worker. The job lease passed to claim protects each individual job separately. A paused handler may still be running after leadership changes; downstream operations must tolerate redelivery. Shared and modulo pollers bypass this gate, so coordinate the rollout across all replicas.

## Verify

Use a dedicated, empty PostgreSQL database. CI provisions PostgreSQL 16 and applies the schema. For local tests apply the schema, set POLLING_TEST_URL to the JDBC URL with test credentials, then run `mvn verify`. Run `mvn install` to install io.pollinglib:kairow:0.1.0-SNAPSHOT locally.

## Automatic polling in Spring Boot 3

Put Kairow on the application's classpath and annotate a public synchronous method on an eager singleton Spring bean. Boot discovers the handler at startup, resolves its DataSource and database adapter, and schedules one fixed-delay task per method. The application supplies the database driver and matching schema.

```java
@KairowPoll(table = "kafka_outbox", idColumn = "event_id", payloadColumn = "event_body",
            strategy = KairowPoll.Strategy.SINGLE_ACTIVE, pollDelayMillis = 1000)
public void publish(PollingQueue.Claim event) {
    publisher.sendAndAwaitAcknowledgment(event.id(), event.payload());
}
```

Choose SHARED (default), MODULO or SINGLE_ACTIVE per method. For MODULO set kairow.shard-index per instance and kairow.shard-count across instances, or use the annotation's shardIndex and shardCount strings with literals or Spring placeholders. Set jobLeaseMillis, retryDelayMillis and leaderLeaseMillis as needed. On shutdown future ticks stop; an in-flight handler may finish. JDK proxies must expose the method on their interface. Set kairow.enabled=false if an existing application already schedules annotated handlers manually.

The mapping uses the existing backend column types, status values and defaults, with retry and lease state stored on the source row by default. IDs must be UUIDs. Use ordinary unquoted identifiers; table names may include a schema. Map statusColumn, attemptsColumn, maxAttemptsColumn, availableAtColumn, ownerColumn and claimTokenColumn when their names differ. Apply the corresponding schema/index changes yourself. For single-active MySQL, seed polling_leader.queue_name with the exact configured table string (within its 64-character limit). Use identical table spelling across replicas.

## Ordered outbox processing

```java
@KairowPoll(
    table = "outbox", companionStateTable = "outbox_state",
    aggregateKeyColumn = "aggregate_id", aggregateSequenceColumn = "event_version",
    batchSize = 8, jobLeaseMillis = 60_000,
    retryBackoff = KairowPoll.Backoff.EXPONENTIAL, retryDelayMillis = 1_000,
    maxRetryDelayMillis = 30_000, retryJitter = 0.2,
    nonRetryable = IllegalArgumentException.class)
public void publish(PollingQueue.Claim event) {
    publisher.sendAndAwaitAcknowledgment(event.id(), event.payload());
}
```

**Ordering.** Configure both columns on every consumer of the queue. Use immutable, non-null keys and sequences with a unique index on the pair. Producers must commit each aggregate's events in sequence order, for example by locking the aggregate row in the business transaction. Kairow selects the lowest unfinished committed sequence for each key. A delayed retry, live claim or terminal failure blocks later versions of that aggregate, including across modulo shards. After investigating a failure, explicitly requeue it or mark it resolved. Independent aggregates continue. Delivery is at least once; use the event ID for downstream idempotency.

**Companion state.** For an append-only outbox the supplied migrations backfill a separate queue and install an insert trigger. Each new event and its companion row commit together; the row stores an immutable payload snapshot, ordering columns and operational state. Business-table updates and deletes are outside this insert-based model. Adapt one migration to the actual source column names and types before enabling the poller. In companion mode the annotation's column mappings apply to the companion table. Each source needs its own companion table. Retain completed rows according to your application's replay and audit rules.

- [PostgreSQL migration](src/main/resources/io/pollinglib/examples/postgres-companion.sql)
- [MySQL migration](src/main/resources/io/pollinglib/examples/mysql-companion.sql)

**Batching and shutdown.** Each tick locks up to `batchSize` eligible rows in one transaction and batches the updates. Spring runs one worker per claimed row and waits for that batch before polling again. The default batch size is one; the maximum is 256 per handler. Handlers using larger batches must be thread-safe. Size the connection pool for processing and heartbeat traffic. Job leases renew approximately every third of the lease duration while processing. A database outage or paused JVM can still cause redelivery. The separate single-active leadership lease renews on claim ticks, so allow for the duration of a batch when choosing it. Shutdown stops new ticks and drains running work with heartbeats for `kairow.shutdown-grace-millis` (default 30000). On timeout Kairow requests interruption and stops heartbeats; outstanding claims become recoverable after expiry.

**Retries and metrics.** Fixed delay is the default. Exponential delay doubles per attempt up to the cap. Jitter of 0.2 samples between 80% and 100% of the capped delay. The row's max_attempts bounds deliveries; matching nonRetryable exceptions immediately fail the row. When a single or primary MeterRegistry bean is available, Kairow records kairow.claimed, kairow.completed, kairow.processing.failures, kairow.renewal.failures and kairow.processing.duration with table and handler tags. Processing failures include retryable, terminal and completion errors.

**JUnit coverage.** Run `mvn verify` with PostgreSQL and the base schema. To include MySQL, also provision the MySQL test database as in the CI workflow and set `KAIROW_MYSQL_TEST=true`. The ordered-batch tests apply the actual companion migrations, exercise a competing transaction and verify retry and fencing behavior. The Spring integration test holds a handler open during shutdown and checks renewal, completion and metrics.

## Pluggable data sources and database adapters

```java
@KairowPoll(table = "trade_outbox", dataSource = "${kairow.trade-source}")
public void publishTrade(PollingQueue.Claim trade) {
    publisher.sendAndAwaitAcknowledgment(trade.id(), trade.payload());
}
```

Set `kairow.trade-source=tradesDataSource` to select an existing Spring JDBC DataSource bean. A literal bean name works too. Each handler can select its own pool. Omitting the attribute selects the single or primary DataSource. When several pools exist without a primary, name the pool on each handler. Kairow checks database metadata at startup and uses that same pool for claims, leadership, renewal, retries and completion. Run each queue migration on its selected database.

PostgreSQL and MySQL have built-in backends. An adapter JAR can contribute a `QueueBackendProvider` bean via Spring Boot auto-configuration. Its `supports` method matches the JDBC product name; `create` receives that handler's QueueMapping. One matching provider takes precedence over the built-in adapter. Multiple matches fail startup. Custom adapters supply vendor SQL, migrations, lease renewal and the claim semantics their handlers require. The application's JDBC driver and connection pool still come from the host.
