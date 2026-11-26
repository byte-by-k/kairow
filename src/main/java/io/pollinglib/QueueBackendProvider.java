package io.pollinglib;

/** Register as a Spring bean, directly or from an adapter JAR's Boot auto-configuration. */
public interface QueueBackendProvider {
    boolean supports(String databaseProductName);
    QueueBackend create(QueueMapping mapping);
}
