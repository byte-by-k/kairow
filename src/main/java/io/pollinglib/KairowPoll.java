package io.pollinglib;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface KairowPoll {
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
