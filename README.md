# Kairow

Java 17 JDBC SDK for competing workers on a shared PostgreSQL database. A Spring Boot application can construct one PollingQueue with its pooled DataSource and invoke claim from a scheduled method on each pod. The host supplies the JDBC driver.

## Why Kairow?

Kairow (pronounced *KAI-roh*) blends the Greek word *kairos*, meaning “the right moment,” with *row*, the PostgreSQL record that holds a job. A worker can claim that row when a new job is ready, a retry becomes due, or a previous worker’s lease expires. The name captures what the framework helps a worker find: the right row at the right moment.

## Usage

Apply src/main/resources/io/pollinglib/schema.sql once through your migration tool. Call enqueue with a stable UUID and the same JDBC connection as your business transaction. Duplicate IDs raise a primary key violation.

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

## Verify

Use a dedicated, empty PostgreSQL database. CI provisions PostgreSQL 16 and applies the schema. For local tests apply the schema, set POLLING_TEST_URL to the JDBC URL with test credentials, then run `mvn verify`. Run `mvn install` to install io.pollinglib:kairow:0.1.0-SNAPSHOT locally.
