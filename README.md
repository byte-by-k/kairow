# Kairow

Java 17 JDBC SDK for competing workers on a shared relational database. A Spring Boot application can construct one PollingQueue with its pooled DataSource and invoke claim from a scheduled method on each pod. The host supplies the JDBC driver.

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

Set the lease longer than the handler timeout. A crash or lease expiry permits redelivery. The unique claim token fences queue updates. Make downstream effects idempotent using the job ID. All pods must use one authoritative writable database. Independently writable databases require external coordination for split-brain protection.

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
