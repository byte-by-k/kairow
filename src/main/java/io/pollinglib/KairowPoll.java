package io.pollinglib;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface KairowPoll {
    enum Strategy { SHARED, MODULO, SINGLE_ACTIVE }

    enum Backoff { FIXED, EXPONENTIAL }

    String aggregateKeyColumn() default "";
    String aggregateSequenceColumn() default "";
    String companionStateTable() default "";
    int batchSize() default 1;
    Backoff retryBackoff() default Backoff.FIXED;
    long maxRetryDelayMillis() default Long.MAX_VALUE;
    double retryJitter() default 0;
    Class<? extends Exception>[] nonRetryable() default {};
    Strategy strategy() default Strategy.SHARED;
    long pollDelayMillis() default 1000;
    long jobLeaseMillis() default 120000;
    long leaderLeaseMillis() default 30000;
    long retryDelayMillis() default 30000;
    String shardIndex() default "${kairow.shard-index}";
    String shardCount() default "${kairow.shard-count}";

    String dataSource() default "";
    String table();
    String idColumn() default "id";
    String payloadColumn() default "payload";
    String statusColumn() default "status";
    String attemptsColumn() default "attempts";
    String maxAttemptsColumn() default "max_attempts";
    String availableAtColumn() default "available_at";
    String ownerColumn() default "owner_id";
    String claimTokenColumn() default "claim_token";
    String leaderTable() default "polling_leader";
}
