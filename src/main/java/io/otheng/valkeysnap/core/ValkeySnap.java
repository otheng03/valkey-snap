package io.otheng.valkeysnap.core;

import io.otheng.valkeysnap.config.SnapshotConfig;
import io.otheng.valkeysnap.consumer.SnapshotListener;
import io.otheng.valkeysnap.consumer.SnapshotListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Objects;

/**
 * Main entry point for the Valkey Snapshot replication utility.
 *
 * <p>ValkeySnap connects to a Valkey/Redis server using PSYNC and replicates
 * snapshot data in various formats:
 * <ul>
 *   <li><b>KeyValue mode:</b> Structured key-value events</li>
 *   <li><b>Command mode:</b> Redis commands ready for replay</li>
 *   <li><b>Raw mode:</b> Raw bytes for pass-through forwarding</li>
 * </ul>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * ValkeySnap snap = ValkeySnap.builder()
 *     .host("localhost")
 *     .port(6379)
 *     .listener(new SnapshotListenerAdapter() {
 *         @Override
 *         public void onKeyValue(KeyValueEvent event) {
 *             System.out.println("Key: " + event.keyAsString());
 *         }
 *     })
 *     .build();
 *
 * snap.start(); // Blocks until complete
 * }</pre>
 *
 * <h2>Modes</h2>
 * <p>By default, only KeyValue mode is enabled. Enable other modes via the builder:
 * <pre>{@code
 * ValkeySnap.builder()
 *     .emitKeyValueEvents(true)   // Default: true
 *     .emitCommandEvents(true)    // Default: false
 *     .emitRawBytes(false)        // Default: false
 *     // ...
 * }</pre>
 */
public final class ValkeySnap implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(ValkeySnap.class);

    private final SnapshotConfig config;
    private final SnapshotListener listener;
    private SnapshotReplicator replicator;

    private ValkeySnap(SnapshotConfig config, SnapshotListener listener) {
        this.config = config;
        this.listener = listener;
    }

    /**
     * Creates a new builder for ValkeySnap.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Starts the replication process.
     * This method blocks until replication completes or an error occurs.
     *
     * @throws IOException if connection or replication fails
     */
    public void start() throws IOException {
        logger.info("Starting ValkeySnap replication to {}:{}", config.host(), config.port());
        replicator = new SnapshotReplicator(config, listener);
        replicator.start();
    }

    /**
     * Starts replication in a background thread.
     *
     * @return the background thread
     */
    public Thread startAsync() {
        Thread thread = new Thread(() -> {
            try {
                start();
            } catch (IOException e) {
                logger.error("Replication failed", e);
                listener.onError(e);
            }
        }, "valkey-snap-replicator");
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    /**
     * Stops the replication process.
     */
    public void stop() {
        if (replicator != null) {
            replicator.stop();
        }
    }

    /**
     * Returns true if replication is currently running.
     */
    public boolean isRunning() {
        return replicator != null && replicator.isRunning();
    }

    /**
     * Returns the configuration.
     */
    public SnapshotConfig config() {
        return config;
    }

    @Override
    public void close() throws IOException {
        stop();
        if (replicator != null) {
            replicator.close();
            replicator = null;
        }
    }

    /**
     * Builder for ValkeySnap instances.
     */
    public static final class Builder {
        private String host = "localhost";
        private int port = 6379;
        private String password;
        private Duration connectionTimeout = Duration.ofSeconds(30);
        private Duration readTimeout = Duration.ofSeconds(60);
        private long rateLimitBytesPerSecond = 0;
        private boolean emitKeyValueEvents = true;
        private boolean emitCommandEvents = false;
        private boolean emitRawBytes = false;
        private int commandChunkMaxElements = 1000;
        private int commandChunkMaxBytes = 64 * 1024;
        private String replId = "?";
        private long replOffset = -1;
        private SnapshotListener listener;

        private Builder() {}

        /**
         * Sets the Valkey/Redis server host.
         */
        public Builder host(String host) {
            this.host = Objects.requireNonNull(host);
            return this;
        }

        /**
         * Sets the Valkey/Redis server port.
         */
        public Builder port(int port) {
            this.port = port;
            return this;
        }

        /**
         * Sets the authentication password.
         */
        public Builder password(String password) {
            this.password = password;
            return this;
        }

        /**
         * Sets the connection timeout.
         */
        public Builder connectionTimeout(Duration timeout) {
            this.connectionTimeout = Objects.requireNonNull(timeout);
            return this;
        }

        /**
         * Sets the read timeout.
         */
        public Builder readTimeout(Duration timeout) {
            this.readTimeout = Objects.requireNonNull(timeout);
            return this;
        }

        /**
         * Sets the rate limit in bytes per second (0 = unlimited).
         */
        public Builder rateLimitBytesPerSecond(long bytesPerSecond) {
            this.rateLimitBytesPerSecond = bytesPerSecond;
            return this;
        }

        /**
         * Enables or disables KeyValue event emission.
         */
        public Builder emitKeyValueEvents(boolean emit) {
            this.emitKeyValueEvents = emit;
            return this;
        }

        /**
         * Enables or disables Command event emission.
         */
        public Builder emitCommandEvents(boolean emit) {
            this.emitCommandEvents = emit;
            return this;
        }

        /**
         * Enables or disables raw bytes emission (pass-through mode).
         */
        public Builder emitRawBytes(boolean emit) {
            this.emitRawBytes = emit;
            return this;
        }

        /**
         * Sets the maximum elements per command chunk.
         */
        public Builder commandChunkMaxElements(int maxElements) {
            this.commandChunkMaxElements = maxElements;
            return this;
        }

        /**
         * Sets the maximum bytes per command chunk.
         */
        public Builder commandChunkMaxBytes(int maxBytes) {
            this.commandChunkMaxBytes = maxBytes;
            return this;
        }

        /**
         * Sets the replication ID for partial sync.
         */
        public Builder replId(String replId) {
            this.replId = Objects.requireNonNull(replId);
            return this;
        }

        /**
         * Sets the replication offset for partial sync.
         */
        public Builder replOffset(long offset) {
            this.replOffset = offset;
            return this;
        }

        /**
         * Sets the snapshot listener to receive events.
         */
        public Builder listener(SnapshotListener listener) {
            this.listener = Objects.requireNonNull(listener);
            return this;
        }

        /**
         * Builds the ValkeySnap instance.
         *
         * @return the configured ValkeySnap
         * @throws IllegalStateException if no listener is set
         */
        public ValkeySnap build() {
            if (listener == null) {
                listener = new SnapshotListenerAdapter();
            }

            SnapshotConfig config = SnapshotConfig.builder()
                .host(host)
                .port(port)
                .password(password)
                .connectionTimeout(connectionTimeout)
                .readTimeout(readTimeout)
                .rateLimitBytesPerSecond(rateLimitBytesPerSecond)
                .emitKeyValueEvents(emitKeyValueEvents)
                .emitCommandEvents(emitCommandEvents)
                .emitRawBytes(emitRawBytes)
                .commandChunkMaxElements(commandChunkMaxElements)
                .commandChunkMaxBytes(commandChunkMaxBytes)
                .replId(replId)
                .replOffset(replOffset)
                .build();

            return new ValkeySnap(config, listener);
        }
    }
}
