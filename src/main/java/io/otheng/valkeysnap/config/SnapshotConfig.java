package io.otheng.valkeysnap.config;

import java.time.Duration;
import java.util.Objects;

/**
 * Configuration for Valkey snapshot replication.
 * Use the builder pattern to create instances.
 */
public final class SnapshotConfig {

    private final String host;
    private final int port;
    private final String password;
    private final Duration connectionTimeout;
    private final Duration readTimeout;

    // Rate limiting
    private final long rateLimitBytesPerSecond;

    // Event emission flags
    private final boolean emitKeyValueEvents;
    private final boolean emitCommandEvents;
    private final boolean emitRawBytes;

    // Chunking configuration
    private final int commandChunkMaxElements;
    private final int commandChunkMaxBytes;

    // Replication identity
    private final String replId;
    private final long replOffset;

    private SnapshotConfig(Builder builder) {
        this.host = Objects.requireNonNull(builder.host, "host is required");
        this.port = builder.port;
        this.password = builder.password;
        this.connectionTimeout = builder.connectionTimeout;
        this.readTimeout = builder.readTimeout;
        this.rateLimitBytesPerSecond = builder.rateLimitBytesPerSecond;
        this.emitKeyValueEvents = builder.emitKeyValueEvents;
        this.emitCommandEvents = builder.emitCommandEvents;
        this.emitRawBytes = builder.emitRawBytes;
        this.commandChunkMaxElements = builder.commandChunkMaxElements;
        this.commandChunkMaxBytes = builder.commandChunkMaxBytes;
        this.replId = builder.replId;
        this.replOffset = builder.replOffset;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    public String password() {
        return password;
    }

    public Duration connectionTimeout() {
        return connectionTimeout;
    }

    public Duration readTimeout() {
        return readTimeout;
    }

    public long rateLimitBytesPerSecond() {
        return rateLimitBytesPerSecond;
    }

    public boolean emitKeyValueEvents() {
        return emitKeyValueEvents;
    }

    public boolean emitCommandEvents() {
        return emitCommandEvents;
    }

    public boolean emitRawBytes() {
        return emitRawBytes;
    }

    public int commandChunkMaxElements() {
        return commandChunkMaxElements;
    }

    public int commandChunkMaxBytes() {
        return commandChunkMaxBytes;
    }

    public String replId() {
        return replId;
    }

    public long replOffset() {
        return replOffset;
    }

    public static final class Builder {
        private String host = "localhost";
        private int port = 6379;
        private String password;
        private Duration connectionTimeout = Duration.ofSeconds(30);
        private Duration readTimeout = Duration.ofSeconds(60);
        private long rateLimitBytesPerSecond = 0; // 0 = unlimited
        private boolean emitKeyValueEvents = true;
        private boolean emitCommandEvents = false;
        private boolean emitRawBytes = false;
        private int commandChunkMaxElements = 1000;
        private int commandChunkMaxBytes = 64 * 1024; // 64KB
        private String replId = "?"; // ? for full sync
        private long replOffset = -1; // -1 for full sync

        private Builder() {}

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("Port must be between 1 and 65535");
            }
            this.port = port;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder connectionTimeout(Duration timeout) {
            this.connectionTimeout = Objects.requireNonNull(timeout);
            return this;
        }

        public Builder readTimeout(Duration timeout) {
            this.readTimeout = Objects.requireNonNull(timeout);
            return this;
        }

        public Builder rateLimitBytesPerSecond(long bytesPerSecond) {
            if (bytesPerSecond < 0) {
                throw new IllegalArgumentException("Rate limit cannot be negative");
            }
            this.rateLimitBytesPerSecond = bytesPerSecond;
            return this;
        }

        public Builder emitKeyValueEvents(boolean emit) {
            this.emitKeyValueEvents = emit;
            return this;
        }

        public Builder emitCommandEvents(boolean emit) {
            this.emitCommandEvents = emit;
            return this;
        }

        public Builder emitRawBytes(boolean emit) {
            this.emitRawBytes = emit;
            return this;
        }

        public Builder commandChunkMaxElements(int maxElements) {
            if (maxElements < 1) {
                throw new IllegalArgumentException("Max elements must be at least 1");
            }
            this.commandChunkMaxElements = maxElements;
            return this;
        }

        public Builder commandChunkMaxBytes(int maxBytes) {
            if (maxBytes < 1) {
                throw new IllegalArgumentException("Max bytes must be at least 1");
            }
            this.commandChunkMaxBytes = maxBytes;
            return this;
        }

        public Builder replId(String replId) {
            this.replId = Objects.requireNonNull(replId);
            return this;
        }

        public Builder replOffset(long offset) {
            this.replOffset = offset;
            return this;
        }

        public SnapshotConfig build() {
            return new SnapshotConfig(this);
        }
    }
}
