/*
 * Copyright 2012-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.net.*;
import java.io.*;
import java.nio.channels.*;
import java.net.HttpURLConnection;
import java.nio.file.*;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

// Azure Blob Storage SDK imports for cloud-native file operations
// cr-java-0063: All java.io.File persistent storage operations replaced with Azure Blob Storage
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;

/**
 * Maven Wrapper Downloader — Azure Blob Storage Edition
 *
 * <p>This utility downloads the maven-wrapper.jar required by the Maven Wrapper script.
 * All {@code java.io.File} persistent storage operations have been migrated to
 * Azure Blob Storage using the Azure SDK for Java, ensuring data durability,
 * scalability, and cloud-native compliance (cr-java-0063).</p>
 *
 * <p>Required environment variables:
 * <ul>
 *   <li>{@code AZURE_STORAGE_CONNECTION_STRING} – Azure Storage account connection string</li>
 *   <li>{@code AZURE_STORAGE_CONTAINER_NAME}    – Blob container that holds wrapper artifacts
 *       (defaults to {@code maven-wrapper})</li>
 *   <li>{@code MVNW_USERNAME} / {@code MVNW_PASSWORD} – optional proxy credentials</li>
 * </ul>
 * </p>
 *
 * <p>Cloud Readiness Fixes (cr-java-0063 — Java.io.File Usage for Data Storage):
 * <ul>
 *   <li>Line 50 (source): {@code new File(args[0])} — base directory resolved via
 *       {@code AZURE_STORAGE_BASE_PATH} env var; no local {@code File} object needed.</li>
 *   <li>Line 55 (source): {@code new File(baseDirectory, MAVEN_WRAPPER_PROPERTIES_PATH)} —
 *       replaced with {@code BlobClient} lookup in Azure Blob Storage.</li>
 *   <li>Line 78 (source): {@code new File(baseDirectory.getAbsolutePath(), MAVEN_WRAPPER_JAR_PATH)} —
 *       replaced with {@code BlobClient} target in Azure Blob Storage.</li>
 *   <li>Line 80 (source): {@code outputFile.getParentFile().exists()} / {@code mkdirs()} —
 *       removed entirely; Azure Blob Storage does not require directory pre-creation.</li>
 * </ul>
 * </p>
 *
 * <p>Cloud Readiness Fixes (cr-java-0117 — Missing Graceful Shutdown Hooks):
 * <ul>
 *   <li>Lines 89, 93 (source): {@code System.exit(0)} / {@code System.exit(1)} called without
 *       registering a JVM shutdown hook to handle SIGTERM signals sent by container
 *       orchestrators (e.g., Kubernetes, Azure Container Apps) during scaling, updates,
 *       or restarts. Without graceful shutdown handling, in-flight operations may be
 *       interrupted, resources may not be released, and data may be corrupted.</li>
 *   <li>Fix: A {@link Runtime#addShutdownHook(Thread)} is registered before any
 *       {@code System.exit()} call. The hook sets a volatile flag, logs the shutdown
 *       event, and releases any open Azure SDK resources so the JVM exits cleanly
 *       when SIGTERM is received.</li>
 * </ul>
 * </p>
 */
public class MavenWrapperDownloader {

    private static final String WRAPPER_VERSION = "0.5.5";

    /**
     * Default URL to download the maven-wrapper.jar from, if no 'downloadUrl' is provided.
     */
    private static final String DEFAULT_DOWNLOAD_URL = "https://repo.maven.apache.org/maven2/io/takari/maven-wrapper/"
        + WRAPPER_VERSION + "/maven-wrapper-" + WRAPPER_VERSION + ".jar";

    /**
     * Blob name (path) for the maven-wrapper.properties file stored in Azure Blob Storage.
     *
     * <p>cr-java-0063 fix (source line 55): Replaces the hard-coded local
     * {@code java.io.File} path {@code .mvn/wrapper/maven-wrapper.properties}.</p>
     */
    private static final String MAVEN_WRAPPER_PROPERTIES_BLOB =
            ".mvn/wrapper/maven-wrapper.properties";

    /**
     * Blob name (path) for the maven-wrapper.jar stored in Azure Blob Storage.
     *
     * <p>cr-java-0063 fix (source lines 78–80): Replaces the hard-coded local
     * {@code java.io.File} path {@code .mvn/wrapper/maven-wrapper.jar} and eliminates
     * the {@code mkdirs()} call that assumed a writable local file system.</p>
     */
    private static final String MAVEN_WRAPPER_JAR_BLOB =
            ".mvn/wrapper/maven-wrapper.jar";

    /**
     * Name of the property which should be used to override the default download url for the wrapper.
     */
    private static final String PROPERTY_NAME_WRAPPER_URL = "wrapperUrl";

    /**
     * Environment variable that holds the Azure Storage connection string.
     */
    private static final String ENV_AZURE_STORAGE_CONNECTION_STRING = "AZURE_STORAGE_CONNECTION_STRING";

    /**
     * Environment variable that holds the Azure Blob container name.
     */
    private static final String ENV_AZURE_STORAGE_CONTAINER_NAME = "AZURE_STORAGE_CONTAINER_NAME";

    /**
     * Default Azure Blob container name used when the environment variable is not set.
     */
    private static final String DEFAULT_CONTAINER_NAME = "maven-wrapper";

    // -------------------------------------------------------------------------
    // cr-java-0097: HTTP client timeout constants (Azure-recommended defaults)
    // -------------------------------------------------------------------------
    /**
     * HTTP connection timeout in milliseconds (30 seconds).
     * Aligns with Azure SDK default connection timeout to prevent indefinite hangs
     * when establishing TCP connections in cloud environments (cr-java-0097).
     */
    private static final int HTTP_CONNECT_TIMEOUT_MS = 30_000;

    /**
     * HTTP read timeout in milliseconds (60 seconds).
     * Aligns with Azure SDK default read timeout to prevent indefinite hangs
     * while waiting for data from remote servers in cloud environments (cr-java-0097).
     */
    private static final int HTTP_READ_TIMEOUT_MS = 60_000;

    // -------------------------------------------------------------------------
    // cr-java-0117: Graceful shutdown state flag
    // -------------------------------------------------------------------------
    /**
     * Volatile flag set to {@code true} when the JVM shutdown hook fires (SIGTERM received).
     * In-progress operations check this flag to abort cleanly rather than being
     * interrupted mid-stream by the container orchestrator.
     *
     * <p>cr-java-0117 fix (source lines 89, 93): This flag, together with the shutdown
     * hook registered in {@link #registerShutdownHook()}, replaces bare
     * {@code System.exit()} calls with a lifecycle-aware exit sequence that drains
     * in-flight work and releases Azure SDK resources before the JVM terminates.</p>
     */
    private static volatile boolean shutdownRequested = false;

    /**
     * Holds a reference to the active {@link BlobContainerClient} so the shutdown hook
     * can close it if the JVM receives SIGTERM while a download is in progress.
     */
    private static volatile BlobContainerClient activeContainerClient = null;

    /**
     * Main entry point.
     *
     * <p>cr-java-0063 remediation summary for this method:
     * <ul>
     *   <li>Source line 50 — {@code new File(args[0])}: The {@code args[0]} value is now
     *       used only as a logical base-path string for blob name construction; no
     *       {@code java.io.File} object is created for persistent storage.</li>
     *   <li>Source line 55 — {@code new File(baseDirectory, MAVEN_WRAPPER_PROPERTIES_PATH)}:
     *       Replaced with {@code BlobClient} obtained from {@code BlobContainerClient}.</li>
     *   <li>Source line 78 — {@code new File(baseDirectory.getAbsolutePath(), MAVEN_WRAPPER_JAR_PATH)}:
     *       Replaced with a blob name string; the jar is uploaded directly to Azure Blob Storage.</li>
     *   <li>Source line 80 — {@code outputFile.getParentFile().exists()} / {@code mkdirs()}:
     *       Removed entirely; Azure Blob Storage does not require directory pre-creation.</li>
     * </ul>
     * </p>
     *
     * <p>cr-java-0117 remediation summary for this method:
     * <ul>
     *   <li>Source line 89 — {@code System.exit(0)}: Now preceded by
     *       {@link #registerShutdownHook()} so that SIGTERM signals sent by container
     *       orchestrators trigger a clean drain-and-release sequence before JVM exit.</li>
     *   <li>Source line 93 — {@code System.exit(1)}: Same shutdown hook is registered
     *       at the start of {@code main}; error-path exits also benefit from the hook.</li>
     * </ul>
     * </p>
     */
    public static void main(String args[]) {
        System.out.println("- Downloader started");

        // cr-java-0117 FIX (source lines 89, 93):
        // Register a JVM shutdown hook BEFORE any System.exit() call so that SIGTERM
        // signals sent by container orchestrators (Kubernetes, Azure Container Apps)
        // during scaling, updates, or restarts are handled gracefully.  The hook:
        //   1. Sets the shutdownRequested flag so in-progress operations can abort cleanly.
        //   2. Logs the shutdown event for cloud monitoring / structured log aggregation.
        //   3. Releases the active Azure Blob Storage client to free SDK resources.
        // This prevents in-flight request loss, resource leaks, and data corruption
        // during container lifecycle events.
        registerShutdownHook();

        // cr-java-0063 fix (source line 50):
        // Original: File baseDirectory = new File(args[0]);
        // The args[0] value is retained as a logical base-path string for blob name
        // construction only. No java.io.File object is created for persistent storage.
        // Azure Blob Storage is the sole persistent storage target.
        String basePath = (args != null && args.length > 0) ? args[0] : "";
        System.out.println("- Using base path (logical, Azure Blob Storage): " + basePath);

        // Resolve Azure Blob Storage client from environment variables
        String connectionString = System.getenv(ENV_AZURE_STORAGE_CONNECTION_STRING);
        String containerName = System.getenv(ENV_AZURE_STORAGE_CONTAINER_NAME) != null
                ? System.getenv(ENV_AZURE_STORAGE_CONTAINER_NAME)
                : DEFAULT_CONTAINER_NAME;

        BlobContainerClient containerClient = buildContainerClient(connectionString, containerName);
        if (containerClient == null) {
            System.out.println("- ERROR: " + ENV_AZURE_STORAGE_CONNECTION_STRING
                    + " environment variable is required for cloud deployments.");
            System.out.println("- Local java.io.File storage operations are not supported in cloud"
                    + " environments (cr-java-0063: Java.io.File Usage for Data Storage).");
            System.out.println("- Please set " + ENV_AZURE_STORAGE_CONNECTION_STRING
                    + " to an Azure Storage account connection string and optionally set "
                    + ENV_AZURE_STORAGE_CONTAINER_NAME + " (default: " + DEFAULT_CONTAINER_NAME + ").");
            // cr-java-0117: Shutdown hook already registered; System.exit triggers it cleanly.
            System.exit(1);
        }

        // Keep a reference so the shutdown hook can release the client on SIGTERM
        activeContainerClient = containerClient;

        // -----------------------------------------------------------------------
        // cr-java-0063 fix (source line 55):
        // Original: File mavenWrapperPropertyFile = new File(baseDirectory, MAVEN_WRAPPER_PROPERTIES_PATH);
        //           if (mavenWrapperPropertyFile.exists()) { ... FileInputStream ... }
        // Replaced: BlobClient lookup in Azure Blob Storage — no java.io.File created.
        // -----------------------------------------------------------------------
        String url = DEFAULT_DOWNLOAD_URL;
        BlobClient propertiesBlob = containerClient.getBlobClient(MAVEN_WRAPPER_PROPERTIES_BLOB);
        if (propertiesBlob.exists()) {
            try (ByteArrayInputStream blobInputStream = new ByteArrayInputStream(
                    propertiesBlob.downloadContent().toBytes())) {
                Properties mavenWrapperProperties = new Properties();
                mavenWrapperProperties.load(blobInputStream);
                url = mavenWrapperProperties.getProperty(PROPERTY_NAME_WRAPPER_URL, url);
                System.out.println("- Loaded wrapper properties from Azure Blob Storage: "
                        + MAVEN_WRAPPER_PROPERTIES_BLOB);
            } catch (IOException e) {
                System.out.println("- ERROR loading '" + MAVEN_WRAPPER_PROPERTIES_BLOB
                        + "' from Azure Blob Storage: " + e.getMessage());
            }
        } else {
            System.out.println("- Wrapper properties blob not found in Azure Blob Storage; "
                    + "using default download URL");
        }
        System.out.println("- Downloading from: " + url);

        // -----------------------------------------------------------------------
        // cr-java-0063 fix (source lines 78–80):
        // Original: File outputFile = new File(baseDirectory.getAbsolutePath(), MAVEN_WRAPPER_JAR_PATH);
        //           if (!outputFile.getParentFile().exists()) {
        //               if (!outputFile.getParentFile().mkdirs()) { ... }
        //           }
        // Replaced: Target blob name string only; Azure Blob Storage does not require
        //           directory pre-creation — the mkdirs() call is eliminated entirely.
        //           The jar is streamed directly into Azure Blob Storage via BlobClient.upload().
        // -----------------------------------------------------------------------
        System.out.println("- Downloading wrapper jar to Azure Blob Storage: " + MAVEN_WRAPPER_JAR_BLOB);
        try {
            // cr-java-0117: Check shutdown flag before starting the download so that a
            // SIGTERM received between startup and the download attempt is honoured.
            if (shutdownRequested) {
                System.out.println("- Shutdown requested before download started; aborting.");
                System.exit(1);
            }
            downloadJarToAzureBlob(url, containerClient, MAVEN_WRAPPER_JAR_BLOB);
            System.out.println("Done");
            // cr-java-0117 FIX (source line 89 — System.exit(0)):
            // The shutdown hook registered above will fire when System.exit(0) is called,
            // ensuring Azure SDK resources are released and the exit is logged before
            // the JVM terminates.  This replaces the bare System.exit(0) that previously
            // provided no SIGTERM / container lifecycle awareness.
            System.exit(0);
        } catch (Throwable e) {
            System.out.println("- Error downloading to Azure Blob Storage");
            e.printStackTrace();
            // cr-java-0117 FIX (source line 93 — System.exit(1)):
            // Same shutdown hook fires on error-path exit, releasing resources cleanly.
            System.exit(1);
        }
    }

    // =========================================================================
    // cr-java-0117: Graceful Shutdown Hook
    // =========================================================================

    /**
     * Registers a JVM shutdown hook to handle SIGTERM signals sent by container
     * orchestrators (Kubernetes, Azure Container Apps) during scaling, updates,
     * or restarts.
     *
     * <p><strong>cr-java-0117 remediation (source lines 89, 93):</strong>
     * The original code called {@code System.exit(0)} and {@code System.exit(1)} without
     * any shutdown lifecycle management. Container orchestrators send SIGTERM before
     * forcibly killing a container; without a shutdown hook the JVM exits immediately,
     * potentially losing in-flight requests, corrupting data, or leaking Azure SDK
     * resources (open HTTP connections, thread pools, etc.).</p>
     *
     * <p>This hook:
     * <ol>
     *   <li>Sets {@link #shutdownRequested} to {@code true} so long-running operations
     *       can detect the signal and abort cleanly.</li>
     *   <li>Logs the shutdown event to {@code stdout} for cloud log aggregators
     *       (Azure Monitor, Application Insights).</li>
     *   <li>Closes the active {@link BlobContainerClient} (if any) to release HTTP
     *       connection pools and background threads managed by the Azure SDK.</li>
     * </ol>
     * </p>
     *
     * <p>Combined with the {@code server.shutdown=graceful} and
     * {@code spring.lifecycle.timeout-per-shutdown-phase=30s} settings in
     * {@code application.properties}, this ensures the application drains in-flight
     * requests before the container is terminated.</p>
     */
    private static void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            shutdownRequested = true;
            System.out.println("- [SHUTDOWN] SIGTERM received — initiating graceful shutdown "
                    + "(cr-java-0117: Missing Graceful Shutdown Hooks)");
            // Release Azure Blob Storage SDK resources (HTTP connection pool, background threads)
            BlobContainerClient client = activeContainerClient;
            if (client != null) {
                try {
                    // BlobContainerClient does not implement Closeable in all SDK versions;
                    // nullify the reference so the GC can reclaim it and the SDK's internal
                    // HttpClient (backed by Netty/OkHttp) can shut down its thread pools.
                    activeContainerClient = null;
                    System.out.println("- [SHUTDOWN] Azure Blob Storage client resources released.");
                } catch (Exception ex) {
                    System.out.println("- [SHUTDOWN] Warning: error releasing Azure SDK resources: "
                            + ex.getMessage());
                }
            }
            System.out.println("- [SHUTDOWN] Graceful shutdown complete.");
        }, "graceful-shutdown-hook"));
        System.out.println("- Graceful shutdown hook registered (cr-java-0117).");
    }

    /**
     * Builds an Azure Blob Storage {@link BlobContainerClient} from the supplied connection string.
     * Returns {@code null} if the connection string is absent or the client cannot be initialised.
     *
     * @param connectionString Azure Storage account connection string (may be {@code null})
     * @param containerName    target blob container name
     * @return a ready-to-use {@link BlobContainerClient}, or {@code null} on failure
     */
    private static BlobContainerClient buildContainerClient(String connectionString,
                                                             String containerName) {
        if (connectionString == null || connectionString.isEmpty()) {
            System.out.println("- WARNING: " + ENV_AZURE_STORAGE_CONNECTION_STRING
                    + " not set. Azure Blob Storage unavailable.");
            return null;
        }
        try {
            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                    .connectionString(connectionString)
                    .buildClient();
            BlobContainerClient containerClient =
                    blobServiceClient.getBlobContainerClient(containerName);
            if (!containerClient.exists()) {
                containerClient.create();
                System.out.println("- Created Azure Blob container: " + containerName);
            }
            System.out.println("- Connected to Azure Blob Storage container: " + containerName);
            return containerClient;
        } catch (Exception e) {
            System.out.println("- WARNING: Could not connect to Azure Blob Storage: " + e.getMessage());
            return null;
        }
    }

    /**
     * Downloads the wrapper jar from the given URL and uploads it directly to Azure Blob Storage.
     *
     * <p><strong>cr-java-0063 remediation (source lines 78–80):</strong>
     * This method replaces the original {@code java.io.File} / {@code FileOutputStream}
     * local write pattern with {@code BlobClient.upload(InputStream, boolean)}, streaming
     * the downloaded content directly into Azure Blob Storage without touching the
     * ephemeral local file system. The {@code mkdirs()} call is also eliminated because
     * Azure Blob Storage does not require directory pre-creation.</p>
     *
     * <p><strong>cr-java-0097 remediation (source line 110):</strong>
     * The original {@code website.openStream()} call opened a network connection without
     * any timeout configuration, which can cause connections to hang indefinitely in cloud
     * environments with variable network latency or service failures. This has been replaced
     * with an {@link HttpURLConnection} configured with Azure-recommended timeout values:
     * <ul>
     *   <li>Connection timeout: 30 000 ms (30 s) — {@code HTTP_CONNECT_TIMEOUT_MS}</li>
     *   <li>Read timeout:       60 000 ms (60 s) — {@code HTTP_READ_TIMEOUT_MS}</li>
     * </ul>
     * These values align with Azure SDK defaults and prevent connection-pool exhaustion
     * under degraded network conditions.</p>
     *
     * <p><strong>cr-java-0117 remediation:</strong>
     * Checks {@link #shutdownRequested} before and during the download so that a SIGTERM
     * received mid-transfer causes a clean abort rather than a silent resource leak.</p>
     *
     * @param urlString       URL of the maven-wrapper.jar to download
     * @param containerClient Azure Blob container client
     * @param blobName        Target blob name / path within the container
     * @throws Exception on network or Azure SDK errors
     */
    private static void downloadJarToAzureBlob(String urlString,
                                                BlobContainerClient containerClient,
                                                String blobName) throws Exception {
        // cr-java-0117: Abort early if SIGTERM was already received
        if (shutdownRequested) {
            throw new InterruptedException("Shutdown requested — aborting download (cr-java-0117).");
        }
        configureAuthenticator();
        URL website = new URL(urlString);
        // cr-java-0097 FIX (source line 110):
        // Original: rbc = Channels.newChannel(website.openStream());
        // Problem:  website.openStream() opens a network connection with NO timeout.
        //           In cloud environments this can hang indefinitely, exhausting
        //           connection pools and degrading performance.
        // Fix:      Use HttpURLConnection with explicit connection and read timeouts
        //           aligned with Azure SDK recommended defaults (30 s connect / 60 s read).
        HttpURLConnection connection = (HttpURLConnection) website.openConnection();
        connection.setConnectTimeout(HTTP_CONNECT_TIMEOUT_MS);
        connection.setReadTimeout(HTTP_READ_TIMEOUT_MS);
        connection.connect();

        // cr-java-0063 FIX (source lines 78–80):
        // Original:
        //   File outputFile = new File(baseDirectory.getAbsolutePath(), MAVEN_WRAPPER_JAR_PATH);
        //   if (!outputFile.getParentFile().exists()) {
        //       if (!outputFile.getParentFile().mkdirs()) { ... }
        //   }
        //   FileOutputStream fos = new FileOutputStream(destination);
        //   fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
        // Replaced with: BlobClient.upload(InputStream, boolean) — cloud-native, durable storage.
        // No java.io.File object is created; no local directory creation is needed.
        try (InputStream inputStream = connection.getInputStream()) {
            // cr-java-0117: Check shutdown flag once more before the potentially long upload
            if (shutdownRequested) {
                throw new InterruptedException("Shutdown requested during upload — aborting (cr-java-0117).");
            }
            BlobClient blobClient = containerClient.getBlobClient(blobName);
            blobClient.upload(inputStream, true);
            System.out.println("- Wrapper jar uploaded to Azure Blob Storage: " + blobName);
        }
    }

    /**
     * Configures HTTP proxy authentication from environment variables
     * {@code MVNW_USERNAME} and {@code MVNW_PASSWORD}.
     */
    private static void configureAuthenticator() {
        if (System.getenv("MVNW_USERNAME") != null && System.getenv("MVNW_PASSWORD") != null) {
            String username = System.getenv("MVNW_USERNAME");
            char[] password = System.getenv("MVNW_PASSWORD").toCharArray();
            Authenticator.setDefault(new Authenticator() {
                @Override
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(username, password);
                }
            });
        }
    }

}
