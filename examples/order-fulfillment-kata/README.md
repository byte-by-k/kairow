# Kata: fulfill orders in sequence

Build a worker that reserves inventory before shipping an order. An inventory outage for order A must not stop order B. Budget about 30–45 minutes.

The [executable reference solution](../../src/test/java/io/pollinglib/kata/OrderFulfillmentKataTest.java) uses Kairow's public `AnnotatedPoller` API, PostgreSQL, and the repository's real companion migration. It runs in the normal Maven test suite. No Spring application or external inventory service is required.

## Run the reference solution

Prerequisites: JDK 17+, Maven, and Docker (or a dedicated PostgreSQL database). Run these commands from the repository root:

```sh
docker run --name kairow-kata -d \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=polling \
  -p 55432:5432 postgres:16
```

Wait until `docker exec kairow-kata pg_isready -U postgres -d polling` reports that connections are accepted, then run:

```sh
export POLLING_TEST_URL='jdbc:postgresql://localhost:55432/polling?user=postgres&password=postgres'
mvn -Dtest=OrderFulfillmentKataTest test
```

Expected: **3 tests, 0 failures, 0 errors**. The fixture creates a random schema, installs the base and companion migrations, and drops only its own schema afterward. The database user needs permission to create schemas. No manual schema setup is needed for this focused command. Full `mvn verify` also runs the existing tests, which require the base schema in their normal search path as described in the root README.

To remove the disposable database when finished:

```sh
docker rm -f kairow-kata
```

## Your exercise

Start from the passing reference, then delete the annotation and body of `FulfillmentHandler.fulfill`. Keep the tests and fixture unchanged. Rebuild the handler in these steps:

1. Add `@KairowPoll` to use `outbox` with companion table `outbox_state`. Configure `aggregate_id` and `event_version` as the ordering columns.
2. Simulate a temporary inventory outage: throw `IllegalStateException` for `A:reserve` on attempt 1 only. On successful calls, append the payload to `delivered` and attempt number to `attempts`.
3. Reject payload `invalid` with `IllegalArgumentException` and configure that exception as non-retryable.
4. Run the focused test command after each change. Explain why `A:ship` cannot run while `A:reserve` is waiting to retry or has permanently failed.

The reference implementation is intentionally supplied in the same file so the repository stays green; use Git to restore it after practicing.

## What the tests prove

| Scenario | Expected result |
|---|---|
| A's reservation fails once | B completes while A waits |
| A's retry becomes due | A reserves on attempt 2, then ships on attempt 1 |
| A's first event is invalid | A's later event stays READY; B still completes |
| Producer rolls back | Neither the source event nor its companion row remains |

The retry test moves `available_at` forward to the present in its isolated fixture instead of using sleeps. In an application, let the database clock and configured delay determine when a retry is due. `pollOnce` propagates handler exceptions after recording retry or failure, which is why the tests expect exceptions.

## Stretch exercises

- Add a business `orders` table and update it in the same transaction as the outbox insert. Prove both changes roll back together.
- Add a durable receipt table keyed by event ID. Simulate a successful downstream effect followed by worker failure before completion, then prove redelivery does not repeat that effect. The in-memory lists here are observation aids, not production idempotency.
- Add a second worker and demonstrate that it cannot claim an event while the first worker's lease is live. Use `PollingQueue.renew` for a manual worker that needs longer processing time.
- Add an explicit recovery step for a FAILED event and prove later events resume only after it is resolved.

## Boundaries to remember

Ordering is per aggregate, not global. Producers must commit each order's versions in sequence; concurrent producers should serialize on the business order row. Delivery is at least once. The companion trigger covers inserts and snapshots the payload; it does not mirror later source updates or deletes. This kata manually calls one polling tick at a time and does not demonstrate Spring's concurrent batches or automatic lease renewal. See the root README for those features.
