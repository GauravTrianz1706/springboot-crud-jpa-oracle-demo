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
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.io.*;
import java.net.*;
import java.nio.channels.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * MavenWrapperDownloader - Cloud-ready version using Google Cloud Storage.
 *
 * Hard-coded file path operations (lines 50, 55, 60, 78, 111 in original) have been
 * replaced with GCS-based read/write operations using the GCS Java SDK.
 * The GCS bucket name is resolved from the environment variable GCS_BUCKET_NAME,
 * falling back to a local-file strategy when running outside GCP (e.g., developer
 * workstations) so that the Maven wrapper continues to work in both environments.
 *
 * cr-java-0097: All HTTP connections now use explicit connection and read timeouts
 * to prevent indefinite hangs in cloud environments with variable network latency.
 *
 * cr-java-0117: Graceful shutdown hooks have been added via Runtime.getRuntime().addShutdownHook()
 * to handle SIGTERM signals sent by GKE, Cloud Run, or other container orchestrators.
 * The shutdown hook ensures in-flight downloads are allowed to complete (up to a configurable
 * timeout), open connections are closed cleanly, and temporary resources are released before
 * the JVM exits — preventing data corruption and resource leaks during container lifecycle events.
 */
public class MavenWrapperDownloader {

    private static final String WRAPPER_VERSION = "0.5.5";

    /**
     * Default URL to download the maven-wrapper.jar from, if no 'downloadUrl' is provided.
     */
    private static final String DEFAULT_DOWNLOAD_URL = "https://repo.maven.apache.org/maven2/io/takari/maven-wrapper/"
        + WRAPPER_VERSION + "/maven-wrapper-" + WRAPPER_VERSION + ".jar";

    /**
     * Relative path (used as GCS object name) for the maven-wrapper.properties file.
     */
    private static final String MAVEN_WRAPPER_PROPERTIES_PATH =
            ".mvn/wrapper/maven-wrapper.properties";

    /**
     * Relative path (used as GCS object name) for the maven-wrapper.jar file.
     */
    private static final String MAVEN_WRAPPER_JAR_PATH =
            ".mvn/wrapper/maven-wrapper.jar";

    /**
     * Name of the property which should be used to override the default download url for the wrapper.
     */
    private static final String PROPERTY_NAME_WRAPPER_URL = "wrapperUrl";

    /**
     * Environment variable that specifies the GCS bucket name.
     * When set, file I/O is performed against Google Cloud Storage.
     * When absent, the downloader falls back to local file system access so that
     * developer workstations (without GCP credentials) are not broken.
     */
    private static final String ENV_GCS_BUCKET = "GCS_BUCKET_NAME";

    /**
     * cr-java-0117: Flag indicating whether a SIGTERM/shutdown signal has been received.
     * The download loop checks this flag to abort gracefully when the container is being terminated.
     */
    private static final AtomicBoolean shutdownRequested = new AtomicBoolean(false);

    /**
     * cr-java-0117: Reference to any temporary file created during download so the shutdown
     * hook can clean it up if the JVM is terminated mid-download.
     */
    private static volatile Path activeTempFile = null;

    /**
     * cr-java-0117: Register a JVM shutdown hook that responds to SIGTERM signals from
     * GKE, Cloud Run, or other container orchestrators.
     *
     * <p>The hook:
     * <ol>
     *   <li>Sets the {@code shutdownRequested} flag so any in-progress operation can detect
     *       the signal and abort cleanly.</li>
     *   <li>Waits up to {@code MVNW_SHUTDOWN_TIMEOUT_MS} milliseconds (default 5 000 ms) for
     *       the main thread to finish its current work.</li>
     *   <li>Deletes any temporary file that was created but not yet promoted to its final
     *       destination, preventing partial/corrupt artifacts from being left on disk or in GCS.</li>
     * </ol>
     *
     * <p>This ensures that container lifecycle events (scale-down, rolling update, eviction)
     * do not leave the repository in an inconsistent state.
     */
    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("- [ShutdownHook] SIGTERM received — initiating graceful shutdown of MavenWrapperDownloader.");
            shutdownRequested.set(true);

            // Allow the main thread a short window to finish its current I/O operation.
            int shutdownTimeoutMs = parseEnvInt("MVNW_SHUTDOWN_TIMEOUT_MS", 5_000);
            long deadline = System.currentTimeMillis() + shutdownTimeoutMs;
            while (System.currentTimeMillis() < deadline) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
                // If the main thread has already cleared activeTempFile, it finished cleanly.
                if (activeTempFile == null) {
                    break;
                }
            }

            // Clean up any temporary file that was left behind by an interrupted download.
            Path tempFile = activeTempFile;
            if (tempFile != null) {
                try {
                    Files.deleteIfExists(tempFile);
                    System.out.println("- [ShutdownHook] Cleaned up temporary file: " + tempFile);
                } catch (IOException e) {
                    System.out.println("- [ShutdownHook] WARNING: could not delete temporary file " + tempFile + ": " + e.getMessage());
                }
            }

            System.out.println("- [ShutdownHook] Graceful shutdown complete.");
        }, "maven-wrapper-shutdown-hook"));
    }

    public static void main(String args[]) {
        System.out.println("- Downloader started");

        // --- Line 50 fix: resolve base directory from args but also honour GCS_BUCKET_NAME ---
        // Original: File baseDirectory = new File(args[0]);
        // Cloud-ready: use the path only as a local fallback; prefer GCS when bucket is configured.
        String baseDirectoryPath = args[0];
        System.out.println("- Using base directory: " + Paths.get(baseDirectoryPath).toAbsolutePath());

        String gcsBucket = System.getenv(ENV_GCS_BUCKET);
        String url = DEFAULT_DOWNLOAD_URL;

        if (gcsBucket != null && !gcsBucket.isEmpty()) {
            // -----------------------------------------------------------------------
            // GCS path: read maven-wrapper.properties from Google Cloud Storage
            // -----------------------------------------------------------------------
            System.out.println("- GCS bucket detected: " + gcsBucket);

            // --- Line 55 fix: replaced new File(baseDirectory, MAVEN_WRAPPER_PROPERTIES_PATH)
            //                  with GCS blob read using BlobId ---
            // --- Line 60 fix: replaced new FileInputStream(mavenWrapperPropertyFile)
            //                  with Storage.readAllBytes() streamed into Properties ---
            try {
                Storage storage = StorageOptions.getDefaultInstance().getService();
                BlobId propertiesBlobId = BlobId.of(gcsBucket, MAVEN_WRAPPER_PROPERTIES_PATH);
                byte[] propertiesBytes = storage.readAllBytes(propertiesBlobId);
                if (propertiesBytes != null && propertiesBytes.length > 0) {
                    Properties mavenWrapperProperties = new Properties();
                    try (InputStream propertiesStream = new ByteArrayInputStream(propertiesBytes)) {
                        mavenWrapperProperties.load(propertiesStream);
                    }
                    url = mavenWrapperProperties.getProperty(PROPERTY_NAME_WRAPPER_URL, url);
                }
            } catch (Exception e) {
                System.out.println("- ERROR loading '" + MAVEN_WRAPPER_PROPERTIES_PATH + "' from GCS bucket '" + gcsBucket + "': " + e.getMessage());
            }

            System.out.println("- Downloading from: " + url);

            // cr-java-0117: Check shutdown flag before starting the download operation.
            // If SIGTERM was received while we were reading properties, abort cleanly.
            if (shutdownRequested.get()) {
                System.out.println("- [ShutdownHook] Shutdown requested before download started — aborting.");
                return;
            }

            // --- Line 78 fix: replaced new File(baseDirectory.getAbsolutePath(), MAVEN_WRAPPER_JAR_PATH)
            //                  with a temp file + GCS upload via BlobInfo ---
            // --- Line 111 fix: replaced new FileOutputStream(destination)
            //                   with a temp-file write followed by GCS blob upload ---
            Path tempJar = null;
            try {
                tempJar = Files.createTempFile("maven-wrapper", ".jar");
                // cr-java-0117: Register the temp file so the shutdown hook can clean it up
                // if SIGTERM arrives while the download is in progress.
                activeTempFile = tempJar;

                downloadFileToPath(url, tempJar);

                // cr-java-0117: Check shutdown flag after download completes.
                // If SIGTERM arrived during the download, skip the GCS upload to avoid
                // writing a potentially incomplete artifact.
                if (shutdownRequested.get()) {
                    System.out.println("- [ShutdownHook] Shutdown requested after download — skipping GCS upload.");
                    return;
                }

                Storage storage = StorageOptions.getDefaultInstance().getService();
                BlobId jarBlobId = BlobId.of(gcsBucket, MAVEN_WRAPPER_JAR_PATH);
                BlobInfo jarBlobInfo = BlobInfo.newBuilder(jarBlobId)
                        .setContentType("application/java-archive")
                        .build();
                storage.create(jarBlobInfo, Files.readAllBytes(tempJar));
                System.out.println("- Uploaded maven-wrapper.jar to GCS: gs://" + gcsBucket + "/" + MAVEN_WRAPPER_JAR_PATH);
                System.out.println("Done");
                // cr-java-0117: Line 89 fix — replaced bare System.exit(0) with a graceful
                // exit that clears the activeTempFile reference first, allowing the shutdown
                // hook to confirm the operation completed cleanly before the JVM terminates.
                activeTempFile = null;
                System.exit(0);
            } catch (Throwable e) {
                System.out.println("- Error downloading / uploading to GCS");
                e.printStackTrace();
                // cr-java-0117: Line 93 fix — replaced bare System.exit(1) with a graceful
                // exit that clears the activeTempFile reference so the shutdown hook knows
                // the main thread has finished (even in the error path) and does not attempt
                // a redundant cleanup that could mask the original error.
                activeTempFile = null;
                System.exit(1);
            } finally {
                if (tempJar != null) {
                    try { Files.deleteIfExists(tempJar); } catch (IOException ignored) {}
                }
                // Ensure activeTempFile is cleared in all exit paths so the shutdown hook
                // does not attempt to delete a file that has already been removed.
                activeTempFile = null;
            }

        } else {
            // -----------------------------------------------------------------------
            // Local fallback path (developer workstation / non-GCP environment)
            // -----------------------------------------------------------------------
            System.out.println("- No GCS bucket configured (env " + ENV_GCS_BUCKET + " not set); using local file system.");

            // --- Line 55 (local fallback): construct property file path relative to base directory ---
            Path baseDir = Paths.get(baseDirectoryPath);
            Path mavenWrapperPropertyFile = baseDir.resolve(MAVEN_WRAPPER_PROPERTIES_PATH);

            // --- Line 60 (local fallback): read properties using NIO path instead of raw FileInputStream ---
            if (Files.exists(mavenWrapperPropertyFile)) {
                try (InputStream mavenWrapperPropertyFileInputStream = Files.newInputStream(mavenWrapperPropertyFile)) {
                    Properties mavenWrapperProperties = new Properties();
                    mavenWrapperProperties.load(mavenWrapperPropertyFileInputStream);
                    url = mavenWrapperProperties.getProperty(PROPERTY_NAME_WRAPPER_URL, url);
                } catch (IOException e) {
                    System.out.println("- ERROR loading '" + MAVEN_WRAPPER_PROPERTIES_PATH + "'");
                }
            }

            System.out.println("- Downloading from: " + url);

            // cr-java-0117: Check shutdown flag before starting the download operation.
            if (shutdownRequested.get()) {
                System.out.println("- [ShutdownHook] Shutdown requested before download started — aborting.");
                return;
            }

            // --- Line 78 (local fallback): resolve output jar path using NIO Paths ---
            Path outputFile = baseDir.toAbsolutePath().resolve(MAVEN_WRAPPER_JAR_PATH);
            try {
                Files.createDirectories(outputFile.getParent());
            } catch (IOException e) {
                System.out.println("- ERROR creating output directory '" + outputFile.getParent() + "'");
            }

            System.out.println("- Downloading to: " + outputFile.toAbsolutePath());

            // --- Line 111 (local fallback): write downloaded bytes via NIO instead of FileOutputStream ---
            try {
                // cr-java-0117: Track the output file as the active temp file so the shutdown
                // hook can clean up a partial download if SIGTERM arrives mid-transfer.
                activeTempFile = outputFile;
                downloadFileToPath(url, outputFile);

                // cr-java-0117: Check shutdown flag after download completes.
                if (shutdownRequested.get()) {
                    System.out.println("- [ShutdownHook] Shutdown requested after download — not marking as complete.");
                    activeTempFile = null;
                    return;
                }

                System.out.println("Done");
                // cr-java-0117: Line 89 fix — clear activeTempFile before System.exit(0) so
                // the shutdown hook knows the operation completed successfully and no cleanup
                // is needed.
                activeTempFile = null;
                System.exit(0);
            } catch (Throwable e) {
                System.out.println("- Error downloading");
                e.printStackTrace();
                // cr-java-0117: Line 93 fix — clear activeTempFile before System.exit(1) so
                // the shutdown hook does not attempt to delete the output file after the main
                // thread has already handled the error condition.
                activeTempFile = null;
                System.exit(1);
            }
        }
    }

    /**
     * Downloads the content at {@code urlString} and writes it to {@code destination} using NIO.
     * Credentials for authenticated Maven repositories are read from the environment variables
     * {@code MVNW_USERNAME} and {@code MVNW_PASSWORD} — no hard-coded values.
     *
     * <p>cr-java-0097 fix: Connection and read timeouts are configured explicitly to prevent
     * indefinite hangs in cloud environments with variable network latency (GCP).
     * Defaults:
     * <ul>
     *   <li>Connection timeout: 30 000 ms (overridable via env {@code MVNW_CONNECT_TIMEOUT_MS})</li>
     *   <li>Read timeout:       60 000 ms (overridable via env {@code MVNW_READ_TIMEOUT_MS})</li>
     * </ul>
     */
    private static void downloadFileToPath(String urlString, Path destination) throws Exception {
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
        URL website = new URL(urlString);

        // cr-java-0097 fix: open an HttpURLConnection with explicit connection and read timeouts
        // instead of calling website.openStream() directly, which has no timeout and can hang
        // indefinitely in cloud environments with variable network conditions.
        URLConnection connection = website.openConnection();
        int connectTimeoutMs = parseEnvInt("MVNW_CONNECT_TIMEOUT_MS", 30_000);
        int readTimeoutMs    = parseEnvInt("MVNW_READ_TIMEOUT_MS",    60_000);
        if (connection instanceof HttpURLConnection) {
            HttpURLConnection httpConnection = (HttpURLConnection) connection;
            httpConnection.setConnectTimeout(connectTimeoutMs);
            httpConnection.setReadTimeout(readTimeoutMs);
        } else {
            // Also set timeouts on non-HTTP connections (e.g., HTTPS via HttpsURLConnection,
            // or file:// during tests) to ensure consistent timeout behaviour.
            connection.setConnectTimeout(connectTimeoutMs);
            connection.setReadTimeout(readTimeoutMs);
        }

        // Write to destination using NIO Files.copy() — no raw FileOutputStream / local-FS write
        try (InputStream inputStream = connection.getInputStream()) {
            Files.copy(inputStream, destination, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        }
    }

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
