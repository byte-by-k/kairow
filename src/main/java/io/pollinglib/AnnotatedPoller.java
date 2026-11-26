package io.pollinglib;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.function.Function;

/** Bind once per handler and call pollOnce from the host scheduler. */
public final class AnnotatedPoller {
    private final Object target;
    private final Method method;
    private final PollingQueue queue;

    public AnnotatedPoller(Object target, String methodName, Function<QueueMapping, PollingQueue> factory)
            throws NoSuchMethodException {
        this.target = target;
        method = target.getClass().getMethod(methodName, PollingQueue.Claim.class);
        var annotation = method.getAnnotation(KairowPoll.class);
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
        try {
            method.invoke(target, claim);
        } catch (InvocationTargetException failure) {
            var cause = failure.getCause();
            if (cause instanceof Error error) throw error;
            try {
                if (!queue.retry(claim, retryDelay)) cause.addSuppressed(new IllegalStateException("Claim expired"));
            } catch (Exception retryFailure) {
                cause.addSuppressed(retryFailure);
            }
            if (cause instanceof Exception exception) throw exception;
            throw failure;
        }
        if (!queue.complete(claim)) throw new IllegalStateException("Claim expired: " + claim.id());
        return true;
    }
}
