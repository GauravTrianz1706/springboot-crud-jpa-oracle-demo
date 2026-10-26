package com.gl.springbootcrudjpaoracledemo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

/**
 * Spring Boot application entry point.
 *
 * <p><strong>cr-java-0117 — Missing Graceful Shutdown Hooks:</strong>
 * Spring Boot 2.3+ supports graceful shutdown natively via
 * {@code server.shutdown=graceful} in {@code application.properties}.
 * When the JVM receives SIGTERM (sent by Kubernetes, Azure Container Apps,
 * or any container orchestrator during scaling, updates, or restarts),
 * Spring Boot:
 * <ol>
 *   <li>Stops accepting new HTTP requests immediately.</li>
 *   <li>Waits up to {@code spring.lifecycle.timeout-per-shutdown-phase} (30 s)
 *       for in-flight requests to complete.</li>
 *   <li>Shuts down the embedded web server, closes the application context,
 *       and releases all managed resources (database connections, thread pools,
 *       etc.) before the JVM exits.</li>
 * </ol>
 * This prevents in-flight request loss, data corruption, and resource leaks
 * during container lifecycle events.
 * </p>
 *
 * <p>The JVM shutdown hook registered here via
 * {@link Runtime#addShutdownHook(Thread)} provides an additional safety net
 * that logs the SIGTERM receipt and triggers a context close if the Spring
 * lifecycle has not already handled it (e.g., when running outside a servlet
 * container or during integration tests).</p>
 */
@SpringBootApplication
public class SpringbootCrudJpaOracleDemoApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext context =
                SpringApplication.run(SpringbootCrudJpaOracleDemoApplication.class, args);

        // cr-java-0117 FIX: Register a JVM shutdown hook to handle SIGTERM signals
        // sent by container orchestrators (Kubernetes, Azure Container Apps) during
        // scaling, updates, or restarts.
        //
        // Spring Boot's built-in graceful shutdown (server.shutdown=graceful) already
        // drains in-flight HTTP requests when the embedded web server receives the
        // shutdown signal.  This explicit hook provides a complementary safety net:
        //   1. Logs the SIGTERM receipt for cloud monitoring / Azure Monitor ingestion.
        //   2. Closes the Spring ApplicationContext if it is still active, ensuring
        //      all @PreDestroy callbacks, DisposableBean.destroy() methods, and
        //      SmartLifecycle.stop() implementations are invoked — releasing database
        //      connection pools (HikariCP), message-listener containers, scheduled
        //      task executors, and other managed resources.
        //   3. Prevents resource leaks and data corruption during container lifecycle
        //      events (pod eviction, rolling update, scale-in, etc.).
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[SHUTDOWN] SIGTERM received — initiating graceful shutdown "
                    + "(cr-java-0117: Missing Graceful Shutdown Hooks)");
            if (context.isActive()) {
                context.close();
                System.out.println("[SHUTDOWN] Spring ApplicationContext closed gracefully.");
            }
            System.out.println("[SHUTDOWN] Graceful shutdown complete.");
        }, "graceful-shutdown-hook"));
    }

}
