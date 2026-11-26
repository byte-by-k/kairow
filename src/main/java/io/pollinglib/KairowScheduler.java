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

    KairowScheduler(ConfigurableListableBeanFactory beans, DataSource source, Environment env) {
        this.beans = beans;
        this.source = source;
        this.env = env;
    }

    private record Task(Runnable run, long delay) {}

    @Override
    public synchronized void start() {
        if (executor != null) return;
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
        return new Task(() -> {
            try {
                poller.pollOnce(owner, Duration.ofMillis(a.jobLeaseMillis()), Duration.ofMillis(a.retryDelayMillis()));
            } catch (Exception e) {
                LOG.log(System.Logger.Level.ERROR, "Kairow poll failed for " + method, e);
            }
        }, a.pollDelayMillis());
    }

    private int shardNumber(String value) {
        return Integer.parseInt(env.resolveRequiredPlaceholders(value));
    }

    @Override
    public synchronized void stop() {
        if (executor != null) executor.shutdown();
        executor = null;
    }

    @Override
    public boolean isRunning() {
        return executor != null;
    }
}
