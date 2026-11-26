package io.pollinglib;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.env.Environment;

final class KairowScheduler implements SmartLifecycle {
    private static final System.Logger LOG = System.getLogger(KairowScheduler.class.getName());
    private final ConfigurableListableBeanFactory beans;
    private final DataSource source;
    private final Environment env;
    private final String owner = UUID.randomUUID().toString();
    private volatile ScheduledThreadPoolExecutor executor;
    private ScheduledThreadPoolExecutor heartbeat;
    private java.util.concurrent.ExecutorService workers;
    private int capacity;
    private volatile boolean stopping;
    private long graceMillis;

    KairowScheduler(ConfigurableListableBeanFactory beans, DataSource source, Environment env) {
        this.beans = beans;
        this.source = source;
        this.env = env;
    }

    private record Task(Runnable run, long delay) {}

    @Override
    public synchronized void start() {
        if (executor != null) return;
        capacity = 0;
        stopping = false;
        graceMillis = env.getProperty("kairow.shutdown-grace-millis", Long.class, 30000L);
        if (graceMillis < 0) throw new IllegalArgumentException("Invalid shutdown grace");
        var tasks = new ArrayList<Task>();
        String database = null;
        for (String name : beans.getSingletonNames()) {
            Object bean = beans.getSingleton(name);
            if (bean == null) continue;
            Map<Method,KairowPoll> methods = MethodIntrospector.selectMethods(AopUtils.getTargetClass(bean),
                    (MethodIntrospector.MetadataLookup<KairowPoll>) method -> method.getAnnotation(KairowPoll.class));
            for (var entry : methods.entrySet()) {
                if (database == null) database = databaseProduct();
                tasks.add(bind(bean, entry.getKey(), entry.getValue(), database));
            }
        }
        workers = java.util.concurrent.Executors.newFixedThreadPool(Math.max(1, capacity));
        heartbeat = new ScheduledThreadPoolExecutor(Math.max(1, capacity));
        heartbeat.setRemoveOnCancelPolicy(true);
        executor = new ScheduledThreadPoolExecutor(Math.max(1, tasks.size()));
        executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        for (var task : tasks) executor.scheduleWithFixedDelay(task.run(), 0, task.delay(), TimeUnit.MILLISECONDS);
    }

    private String databaseProduct() {
        try (var connection = source.getConnection()) {
            return connection.getMetaData().getDatabaseProductName();
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot identify Kairow database", e);
        }
    }

    private Task bind(Object bean, Method method, KairowPoll a, String database) {
        if (!Modifier.isPublic(method.getModifiers()) || Modifier.isStatic(method.getModifiers())
                || method.getParameterCount() != 1 || method.getParameterTypes()[0] != PollingQueue.Claim.class
                || method.getReturnType() != void.class)
            throw new IllegalArgumentException("Expected public void handler(PollingQueue.Claim): " + method);
        if (a.pollDelayMillis() < 1 || a.jobLeaseMillis() < 1 || a.retryDelayMillis() < 0)
            throw new IllegalArgumentException("Invalid poll, lease or retry timing on " + method);
        if (a.batchSize() < 1 || a.batchSize() > 256 || a.maxRetryDelayMillis() < 0 ||
                !(a.retryJitter() >= 0 && a.retryJitter() <= 1))
            throw new IllegalArgumentException("Invalid batch size or retry policy on " + method);
        capacity = Math.addExact(capacity, a.batchSize());
        var invocable = AopUtils.selectInvocableMethod(method, bean.getClass());
        var poller = new AnnotatedPoller(bean, invocable, a, mapping -> {
            var backend = switch (database) {
                case "PostgreSQL" -> QueueBackend.postgresql(mapping);
                case "MySQL" -> QueueBackend.mysql(mapping);
                default -> throw new IllegalArgumentException("Unsupported automatic Kairow backend: " + database);
            };
            return switch (a.strategy()) {
                case SHARED -> new PollingQueue(source, backend);
                case SINGLE_ACTIVE -> PollingQueue.singleActive(source, backend, Duration.ofMillis(a.leaderLeaseMillis()));
                case MODULO -> PollingQueue.modulo(source, backend, shardNumber(a.shardIndex()), shardNumber(a.shardCount()));
            };
        });
        var registry = beans.getBeanProvider(io.micrometer.core.instrument.MeterRegistry.class).getIfUnique();
        var metrics = new PollMetrics(registry, a.table(), method.getName());
        var lease = Duration.ofMillis(a.jobLeaseMillis());
        return new Task(() -> {
            if (stopping) return;
            try {
                var batch = poller.queue.claimBatch(owner, lease, a.batchSize());
                metrics.claimed(batch.size());
                var jobs = new ArrayList<java.util.concurrent.Callable<Void>>();
                for (var claim : batch) jobs.add(() -> {
                    processClaim(poller, claim, a, lease, metrics);
                    return null;
                });
                for (var result : workers.invokeAll(jobs)) result.get();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } catch (Exception failure) {
                LOG.log(System.Logger.Level.ERROR, "Kairow poll failed for " + method, failure);
            }
        }, a.pollDelayMillis());
    }

    private void processClaim(AnnotatedPoller poller, PollingQueue.Claim claim, KairowPoll options,
                              Duration lease, PollMetrics metrics) {
        long interval = Math.max(1, lease.toMillis() / 3);
        var renewal = heartbeat.scheduleWithFixedDelay(() -> {
            try {
                if (!poller.queue.renew(claim, lease)) metrics.renewalFailure();
            } catch (SQLException failure) {
                metrics.renewalFailure();
                LOG.log(System.Logger.Level.WARNING, "Lease renewal failed for " + claim.id(), failure);
            }
        }, interval, interval, TimeUnit.MILLISECONDS);
        long started = System.nanoTime();
        try {
            poller.process(claim, Duration.ofMillis(options.retryDelayMillis()));
            metrics.completed();
        } catch (Exception failure) {
            metrics.failure();
            LOG.log(System.Logger.Level.ERROR, "Handler failed for " + claim.id(), failure);
        } finally {
            renewal.cancel(false);
            metrics.elapsed(System.nanoTime() - started);
        }
    }

    private int shardNumber(String value) {
        return Integer.parseInt(env.resolveRequiredPlaceholders(value));
    }

    @Override
    public synchronized void stop() {
        if (executor == null) return;
        stopping = true;
        executor.shutdown();
        try {
            if (!executor.awaitTermination(graceMillis, TimeUnit.MILLISECONDS)) executor.shutdownNow();
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        } finally {
            workers.shutdownNow();
            heartbeat.shutdownNow();
        }
        executor = null;
    }

    @Override
    public boolean isRunning() {
        return executor != null;
    }
}
