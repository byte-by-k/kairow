package io.pollinglib;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.function.Function;

/** Bind once per handler and call pollOnce from the host scheduler. */
public final class AnnotatedPoller {
    private final Object target;
    private final Method method;
    final PollingQueue queue;
    private final KairowPoll options;

    public AnnotatedPoller(Object target, String methodName, Function<QueueMapping, PollingQueue> factory)
            throws NoSuchMethodException {
        this(target, target.getClass().getMethod(methodName, PollingQueue.Claim.class),
                target.getClass().getMethod(methodName, PollingQueue.Claim.class).getAnnotation(KairowPoll.class), factory);
    }

    AnnotatedPoller(Object target, Method method, KairowPoll annotation, Function<QueueMapping, PollingQueue> factory) {
        this.options = annotation;
        this.target = target;
        this.method = method;
        if (annotation == null || method.getReturnType() != void.class || !method.trySetAccessible())
            throw new IllegalArgumentException("Expected accessible @KairowPoll void handler(Claim)");
        queue = factory.apply(QueueMapping.from(annotation));
    }

    /** Returns false for an idle tick. Handler failures are retried and propagated. */
    public boolean pollOnce(String owner, Duration lease, Duration retryDelay) throws Exception {
        if (retryDelay.isNegative()) throw new IllegalArgumentException("Negative retry delay");
        var next = queue.claim(owner, lease);
        if (next.isEmpty()) return false;
        var claim = next.get();
        process(claim, retryDelay);
        return true;
    }

    void process(PollingQueue.Claim claim, Duration retryDelay) throws Exception {
        try {
            method.invoke(target, claim);
        } catch (InvocationTargetException failure) {
            var cause = failure.getCause();
            if (cause instanceof Error error) throw error;
            try {
                boolean terminal = java.util.Arrays.stream(options.nonRetryable()).anyMatch(t -> t.isInstance(cause));
                boolean updated = terminal ? queue.fail(claim) : queue.retry(claim,
                        retryDelay(options, claim.attempt(), retryDelay));
                if (!updated) cause.addSuppressed(new IllegalStateException("Claim expired"));
            } catch (Exception retryFailure) {
                cause.addSuppressed(retryFailure);
            }
            if (cause instanceof Exception exception) throw exception;
            throw failure;
        }
        if (!queue.complete(claim)) throw new IllegalStateException("Claim expired: " + claim.id());
    }

    static Duration retryDelay(KairowPoll options, int attempt, Duration base) {
        double value = base.toMillis();
        if (options.retryBackoff() == KairowPoll.Backoff.EXPONENTIAL && value > 0)
            value = Math.scalb(value, Math.max(0, attempt - 1));
        value = Math.min(value, options.maxRetryDelayMillis());
        if (options.retryJitter() > 0)
            value *= 1 - java.util.concurrent.ThreadLocalRandom.current().nextDouble() * options.retryJitter();
        return Duration.ofMillis((long)value);
    }
}
