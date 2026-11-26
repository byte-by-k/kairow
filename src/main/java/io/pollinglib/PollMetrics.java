package io.pollinglib;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.concurrent.TimeUnit;

final class PollMetrics {
    private final Counter claims, completions, failures, renewFailures;
    private final Timer timer;

    PollMetrics(MeterRegistry registry, String table, String handler) {
        String[] tags = {"table", table, "handler", handler};
        claims = registry == null ? null : registry.counter("kairow.claimed", tags);
        completions = registry == null ? null : registry.counter("kairow.completed", tags);
        failures = registry == null ? null : registry.counter("kairow.processing.failures", tags);
        renewFailures = registry == null ? null : registry.counter("kairow.renewal.failures", tags);
        timer = registry == null ? null : registry.timer("kairow.processing.duration", tags);
    }

    void claimed(int size) { if (claims != null) claims.increment(size); }
    void completed() { if (completions != null) completions.increment(); }
    void failure() { if (failures != null) failures.increment(); }
    void renewalFailure() { if (renewFailures != null) renewFailures.increment(); }
    void elapsed(long nanos) { if (timer != null) timer.record(nanos, TimeUnit.NANOSECONDS); }
}
