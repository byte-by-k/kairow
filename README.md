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

## Verify

Use a dedicated, empty PostgreSQL database. CI provisions PostgreSQL 16 and applies the schema. For local tests apply the schema, set POLLING_TEST_URL to the JDBC URL with test credentials, then run `mvn verify`. Run `mvn install` to install io.pollinglib:kairow:0.1.0-SNAPSHOT locally.
