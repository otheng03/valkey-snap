package io.otheng.valkeysnap.network;

import io.otheng.valkeysnap.config.SnapshotConfig;
import io.otheng.valkeysnap.protocol.resp.RespParser;
import io.otheng.valkeysnap.protocol.resp.RespValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Handles the PSYNC replication handshake with a Valkey/Redis server.
 *
 * The handshake sequence is:
 * 1. AUTH (if password configured)
 * 2. PING
 * 3. REPLCONF listening-port
 * 4. REPLCONF capa eof capa psync2
 * 5. PSYNC replid offset
 */
public class ReplicationHandshake {

    private static final Logger logger = LoggerFactory.getLogger(ReplicationHandshake.class);

    private final ValkeyConnection connection;
    private final SnapshotConfig config;
    private final RespParser parser;

    // Results from handshake
    private String masterReplId;
    private long masterReplOffset;
    private boolean fullSync;

    public ReplicationHandshake(ValkeyConnection connection, SnapshotConfig config) {
        this.connection = connection;
        this.config = config;
        this.parser = new RespParser(connection.getInputStream());
    }

    /**
     * Performs the complete PSYNC handshake.
     *
     * @throws IOException if handshake fails
     * @throws HandshakeException if server returns an error
     */
    public void performHandshake() throws IOException {
        // Step 1: AUTH if password is configured
        if (config.password() != null && !config.password().isEmpty()) {
            authenticate();
        }

        // Step 2: PING to verify connection
        ping();

        // Step 3: REPLCONF listening-port (we use 0 since we're not a real replica)
        replconfListeningPort();

        // Step 4: REPLCONF capa
        replconfCapabilities();

        // Step 5: PSYNC
        psync();

        logger.info("Handshake complete. Master replid={}, offset={}, fullSync={}",
                masterReplId, masterReplOffset, fullSync);
    }

    private void authenticate() throws IOException {
        logger.debug("Sending AUTH");
        connection.sendCommand("AUTH", config.password());
        RespValue response = parser.parse();
        expectOk(response, "AUTH");
        logger.info("Authentication successful");
    }

    private void ping() throws IOException {
        logger.debug("Sending PING");
        connection.sendCommand("PING");
        RespValue response = parser.parse();

        // Accept both +PONG and $4\r\nPONG\r\n
        if (response instanceof RespValue.SimpleString ss) {
            if (!"PONG".equals(ss.value())) {
                throw new HandshakeException("Expected PONG, got: " + ss.value());
            }
        } else if (response instanceof RespValue.BulkString bs) {
            if (!"PONG".equals(bs.asString())) {
                throw new HandshakeException("Expected PONG, got: " + bs.asString());
            }
        } else if (response instanceof RespValue.Error err) {
            throw new HandshakeException("PING failed: " + err.message());
        } else {
            throw new HandshakeException("Unexpected PING response: " + response);
        }

        logger.debug("PING successful");
    }

    private void replconfListeningPort() throws IOException {
        logger.debug("Sending REPLCONF listening-port");
        connection.sendCommand("REPLCONF", "listening-port", "0");
        RespValue response = parser.parse();
        expectOk(response, "REPLCONF listening-port");
    }

    private void replconfCapabilities() throws IOException {
        logger.debug("Sending REPLCONF capa");
        connection.sendCommand("REPLCONF", "capa", "eof", "capa", "psync2");
        RespValue response = parser.parse();
        expectOk(response, "REPLCONF capa");
    }

    private void psync() throws IOException {
        String replId = config.replId();
        long replOffset = config.replOffset();

        logger.debug("Sending PSYNC {} {}", replId, replOffset);
        connection.sendCommand("PSYNC", replId, String.valueOf(replOffset));

        // Parse PSYNC response - this can be:
        // +FULLRESYNC <replid> <offset>
        // +CONTINUE
        // +CONTINUE <replid>
        // -ERR ...
        RespValue response = parser.parse();

        if (response instanceof RespValue.SimpleString ss) {
            String value = ss.value();
            if (value.startsWith("FULLRESYNC")) {
                parseFullResync(value);
            } else if (value.startsWith("CONTINUE")) {
                parseContinue(value);
            } else {
                throw new HandshakeException("Unexpected PSYNC response: " + value);
            }
        } else if (response instanceof RespValue.Error err) {
            throw new HandshakeException("PSYNC failed: " + err.message());
        } else {
            throw new HandshakeException("Unexpected PSYNC response type: " + response.type());
        }
    }

    private void parseFullResync(String response) {
        // Format: FULLRESYNC <replid> <offset>
        String[] parts = response.split(" ");
        if (parts.length < 3) {
            throw new HandshakeException("Invalid FULLRESYNC response: " + response);
        }

        this.masterReplId = parts[1];
        this.masterReplOffset = Long.parseLong(parts[2]);
        this.fullSync = true;

        logger.info("Full resync requested. replid={}, offset={}", masterReplId, masterReplOffset);
    }

    private void parseContinue(String response) {
        // Format: CONTINUE or CONTINUE <new-replid>
        String[] parts = response.split(" ");

        if (parts.length > 1) {
            this.masterReplId = parts[1];
        } else {
            this.masterReplId = config.replId();
        }
        this.masterReplOffset = config.replOffset();
        this.fullSync = false;

        logger.info("Partial resync. replid={}, offset={}", masterReplId, masterReplOffset);
    }

    private void expectOk(RespValue response, String command) throws HandshakeException {
        if (response instanceof RespValue.SimpleString ss) {
            if (!"OK".equals(ss.value())) {
                throw new HandshakeException(command + " failed: " + ss.value());
            }
        } else if (response instanceof RespValue.Error err) {
            throw new HandshakeException(command + " failed: " + err.message());
        } else {
            throw new HandshakeException(command + " unexpected response: " + response);
        }
    }

    /**
     * Returns the master's replication ID after handshake.
     */
    public String getMasterReplId() {
        return masterReplId;
    }

    /**
     * Returns the master's replication offset after handshake.
     */
    public long getMasterReplOffset() {
        return masterReplOffset;
    }

    /**
     * Returns true if this is a full sync (FULLRESYNC), false for partial sync (CONTINUE).
     */
    public boolean isFullSync() {
        return fullSync;
    }

    /**
     * Exception thrown when handshake fails.
     */
    public static class HandshakeException extends RuntimeException {
        public HandshakeException(String message) {
            super(message);
        }

        public HandshakeException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
