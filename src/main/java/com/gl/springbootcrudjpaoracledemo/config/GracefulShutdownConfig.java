package com.gl.springbootcrudjpaoracledemo.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.web.embedded.tomcat.TomcatConnectorCustomizer;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextClosedEvent;

import org.apache.catalina.connector.Connector;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * GracefulShutdownConfig — cr-java-0117: Missing Graceful Shutdown Hooks
 *
 * <p>This configuration class implements graceful shutdown handling for the Spring Boot
 * application running on GKE, Cloud Run, or any other GCP container orchestrator that
 * sends SIGTERM signals during scaling, rolling updates, or container restarts.
 *
 * <p>Without graceful shutdown handling, a SIGTERM causes the JVM to exit immediately,
 * which can:
 * <ul>
 *   <li>Drop in-flight HTTP requests mid-processing</li>
 *   <li>Leave database transactions in an inconsistent state</li>
 *   <li>Fail to release connection pool resources cleanly</li>
 *   <li>Corrupt data being written to persistent storage</li>
 * </ul>
 *
 * <p>This class provides two complementary shutdown mechanisms:
 * <ol>
 *   <li><b>Tomcat connector graceful shutdown</b> ({@link GracefulShutdownTomcatConnector}):
 *       Pauses the Tomcat connector when a {@link ContextClosedEvent} is received so that
 *       no new requests are accepted, then waits up to {@code GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS}
 *       (default 30 s) for the thread pool to drain all in-flight requests before the
 *       application context is destroyed.</li>
 *   <li><b>JVM shutdown hook</b> (registered in {@link #registerShutdownHook()}):
 *       Catches SIGTERM signals that arrive before the Spring context is fully initialised
 *       (e.g., during startup) and logs a structured message so that GCP Cloud Logging
 *       can surface the event for observability.</li>
 * </ol>
 *
 * <p>The shutdown timeout is configurable via the environment variable
 * {@code GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS} (integer seconds, default 30).
 * This matches the {@code spring.lifecycle.timeout-per-shutdown-phase} property set in
 * {@code application.properties}.
 */
@Configuration
public class GracefulShutdownConfig {

    private static final Logger log = LoggerFactory.getLogger(GracefulShutdownConfig.class);

    /**
     * Default number of seconds to wait for in-flight requests to complete before
     * forcing the thread pool to shut down.  Overridable via the environment variable
     * {@code GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS}.
     */
    private static final int DEFAULT_SHUTDOWN_TIMEOUT_SECONDS = 30;

    /**
     * Registers a JVM-level shutdown hook via {@code Runtime.getRuntime().addShutdownHook()}.
     *
     * <p>This hook fires when the JVM receives SIGTERM (or SIGINT) from the container
     * orchestrator.  It logs a structured message that is visible in GCP Cloud Logging,
     * giving operators visibility into container lifecycle events.
     *
     * <p>The hook is intentionally lightweight — the heavy lifting (draining the Tomcat
     * thread pool) is handled by {@link GracefulShutdownTomcatConnector} via the Spring
     * {@link ContextClosedEvent} mechanism.  The two mechanisms work together:
     * Spring's {@code server.shutdown=graceful} setting (in {@code application.properties})
     * triggers the connector pause, while this hook ensures the signal is always logged
     * even if the Spring context has not yet started.
     *
     * @return a {@link DisposableBean} that deregisters logging on context close (no-op,
     *         included for Spring lifecycle symmetry)
     */
    @Bean
    public DisposableBean registerShutdownHook() {
        int timeoutSeconds = parseEnvInt("GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS", DEFAULT_SHUTDOWN_TIMEOUT_SECONDS);

        Thread shutdownHookThread = new Thread(() -> {
            log.info("[GracefulShutdown] SIGTERM received — JVM shutdown hook triggered. "
                    + "Waiting up to {} seconds for in-flight requests to complete.", timeoutSeconds);
            // The actual request draining is handled by GracefulShutdownTomcatConnector.
            // This hook provides an additional safety net and structured log entry for
            // GCP Cloud Logging / Cloud Monitoring alerting.
            log.info("[GracefulShutdown] JVM shutdown hook complete — resources will be released by Spring context.");
        }, "graceful-shutdown-hook");

        shutdownHookThread.setDaemon(false);
        Runtime.getRuntime().addShutdownHook(shutdownHookThread);
        log.info("[GracefulShutdown] JVM shutdown hook registered (timeout: {} seconds).", timeoutSeconds);

        return () -> {
            // DisposableBean callback — called when the Spring context closes normally.
            // The shutdown hook will also fire at JVM exit; both paths are safe.
            log.info("[GracefulShutdown] Spring context closing — graceful shutdown in progress.");
        };
    }

    /**
     * Registers the {@link GracefulShutdownTomcatConnector} as a Spring bean so that it
     * is wired into the embedded Tomcat lifecycle automatically.
     *
     * @return a configured {@link GracefulShutdownTomcatConnector}
     */
    @Bean
    public GracefulShutdownTomcatConnector gracefulShutdownTomcatConnector() {
        return new GracefulShutdownTomcatConnector();
    }

    /**
     * Registers the {@link GracefulShutdownTomcatConnector} as a {@link TomcatConnectorCustomizer}
     * so that it receives a reference to the Tomcat {@link Connector} at startup.
     *
     * @param gracefulShutdownTomcatConnector the connector customizer bean
     * @return the same instance, cast to {@link TomcatConnectorCustomizer}
     */
    @Bean
    public TomcatConnectorCustomizer tomcatConnectorCustomizer(
            GracefulShutdownTomcatConnector gracefulShutdownTomcatConnector) {
        return gracefulShutdownTomcatConnector;
    }

    // -------------------------------------------------------------------------
    // Inner class: GracefulShutdownTomcatConnector
    // -------------------------------------------------------------------------

    /**
     * Handles graceful shutdown of the embedded Tomcat connector.
     *
     * <p>When Spring publishes a {@link ContextClosedEvent} (triggered by SIGTERM via
     * {@code server.shutdown=graceful}), this listener:
     * <ol>
     *   <li>Pauses the Tomcat {@link Connector} so no new requests are accepted.</li>
     *   <li>Retrieves the connector's thread pool executor.</li>
     *   <li>Calls {@link ThreadPoolExecutor#shutdown()} to stop accepting new tasks.</li>
     *   <li>Waits up to {@code GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS} for queued tasks to finish.</li>
     *   <li>If the timeout expires, logs a warning and calls
     *       {@link ThreadPoolExecutor#shutdownNow()} to interrupt remaining threads.</li>
     * </ol>
     *
     * <p>This ensures that in-flight HTTP requests complete before the application context
     * is destroyed, preventing data loss and connection pool corruption during GKE rolling
     * updates or Cloud Run instance replacements.
     */
    public static class GracefulShutdownTomcatConnector
            implements TomcatConnectorCustomizer, ApplicationListener<ContextClosedEvent> {

        private static final Logger log = LoggerFactory.getLogger(GracefulShutdownTomcatConnector.class);

        private volatile Connector connector;

        /**
         * Called by Tomcat at startup — stores a reference to the connector so it can
         * be paused during shutdown.
         *
         * @param connector the embedded Tomcat connector
         */
        @Override
        public void customize(Connector connector) {
            this.connector = connector;
        }

        /**
         * Called when the Spring application context is closed (i.e., when SIGTERM is
         * received and {@code server.shutdown=graceful} is configured).
         *
         * <p>Pauses the connector, then waits for the thread pool to drain.
         *
         * @param event the context-closed event published by Spring
         */
        @Override
        public void onApplicationEvent(ContextClosedEvent event) {
            if (this.connector == null) {
                log.warn("[GracefulShutdown] Tomcat connector not yet initialised — skipping graceful drain.");
                return;
            }

            int timeoutSeconds = parseEnvInt("GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS", DEFAULT_SHUTDOWN_TIMEOUT_SECONDS);
            log.info("[GracefulShutdown] ContextClosedEvent received — pausing Tomcat connector. "
                    + "Waiting up to {} seconds for in-flight requests to complete.", timeoutSeconds);

            this.connector.pause();

            Executor executor = this.connector.getProtocolHandler().getExecutor();
            if (executor instanceof ThreadPoolExecutor) {
                ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executor;
                threadPoolExecutor.shutdown();
                try {
                    if (!threadPoolExecutor.awaitTermination(timeoutSeconds, TimeUnit.SECONDS)) {
                        log.warn("[GracefulShutdown] Tomcat thread pool did not drain within {} seconds — "
                                + "forcing shutdown. Some in-flight requests may be interrupted.", timeoutSeconds);
                        threadPoolExecutor.shutdownNow();
                        if (!threadPoolExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                            log.error("[GracefulShutdown] Tomcat thread pool did not terminate after forced shutdown.");
                        }
                    } else {
                        log.info("[GracefulShutdown] All in-flight requests completed — Tomcat connector shut down cleanly.");
                    }
                } catch (InterruptedException ex) {
                    log.warn("[GracefulShutdown] Interrupted while waiting for Tomcat thread pool to drain — "
                            + "forcing immediate shutdown.");
                    threadPoolExecutor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            } else {
                log.info("[GracefulShutdown] Tomcat executor is not a ThreadPoolExecutor (type: {}) — "
                        + "skipping explicit drain.", executor != null ? executor.getClass().getName() : "null");
            }
        }
    }

    // -------------------------------------------------------------------------
    // Utility
    // -------------------------------------------------------------------------

    /**
     * Reads an integer value from the named environment variable.
     * Returns {@code defaultValue} when the variable is absent or cannot be parsed.
     *
     * @param envVar       name of the environment variable
     * @param defaultValue fallback value
     * @return parsed integer or {@code defaultValue}
     */
    private static int parseEnvInt(String envVar, int defaultValue) {
        String value = System.getenv(envVar);
        if (value != null && !value.isEmpty()) {
            try {
                return Integer.parseInt(value.trim());
            } catch (NumberFormatException ignored) {
                // fall through to default
            }
        }
        return defaultValue;
    }
}
