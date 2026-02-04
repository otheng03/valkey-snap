package io.otheng.valkeysnap.network;

import io.otheng.valkeysnap.config.SnapshotConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * Manages TCP connection to a Valkey/Redis server.
 * Provides input/output streams with optional rate limiting.
 */
public class ValkeyConnection implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(ValkeyConnection.class);

    private final SnapshotConfig config;
    private Socket socket;
    private InputStream inputStream;
    private OutputStream outputStream;
    private volatile boolean connected;

    public ValkeyConnection(SnapshotConfig config) {
        this.config = config;
    }

    /**
     * Establishes connection to the Valkey server.
     *
     * @throws IOException if connection fails
     */
    public synchronized void connect() throws IOException {
        if (connected) {
            throw new IllegalStateException("Already connected");
        }

        logger.info("Connecting to {}:{}", config.host(), config.port());

        socket = new Socket();
        socket.setTcpNoDelay(true);
        socket.setKeepAlive(true);
        socket.setSoTimeout((int) config.readTimeout().toMillis());

        socket.connect(
            new InetSocketAddress(config.host(), config.port()),
            (int) config.connectionTimeout().toMillis()
        );

        InputStream rawInput = new BufferedInputStream(socket.getInputStream(), 64 * 1024);

        // Apply rate limiting if configured
        if (config.rateLimitBytesPerSecond() > 0) {
            inputStream = new RateLimitInputStream(rawInput, config.rateLimitBytesPerSecond());
            logger.info("Rate limiting enabled: {} bytes/sec", config.rateLimitBytesPerSecond());
        } else {
            inputStream = rawInput;
        }

        outputStream = new BufferedOutputStream(socket.getOutputStream(), 8 * 1024);
        connected = true;

        logger.info("Connected to {}:{}", config.host(), config.port());
    }

    /**
     * Returns the input stream for reading from the server.
     */
    public InputStream getInputStream() {
        ensureConnected();
        return inputStream;
    }

    /**
     * Returns the output stream for writing to the server.
     */
    public OutputStream getOutputStream() {
        ensureConnected();
        return outputStream;
    }

    /**
     * Sends a RESP command to the server.
     *
     * @param args command arguments (first is the command name)
     * @throws IOException if writing fails
     */
    public void sendCommand(String... args) throws IOException {
        ensureConnected();

        StringBuilder sb = new StringBuilder();
        sb.append('*').append(args.length).append("\r\n");
        for (String arg : args) {
            byte[] bytes = arg.getBytes(StandardCharsets.UTF_8);
            sb.append('$').append(bytes.length).append("\r\n");
            sb.append(arg).append("\r\n");
        }

        byte[] command = sb.toString().getBytes(StandardCharsets.UTF_8);
        outputStream.write(command);
        outputStream.flush();

        if (logger.isDebugEnabled()) {
            logger.debug("Sent command: {}", args[0]);
        }
    }

    /**
     * Sends raw bytes to the server.
     *
     * @param data the bytes to send
     * @throws IOException if writing fails
     */
    public void sendRaw(byte[] data) throws IOException {
        ensureConnected();
        outputStream.write(data);
        outputStream.flush();
    }

    /**
     * Checks if the connection is established.
     */
    public boolean isConnected() {
        return connected && socket != null && socket.isConnected() && !socket.isClosed();
    }

    private void ensureConnected() {
        if (!connected) {
            throw new IllegalStateException("Not connected");
        }
    }

    @Override
    public synchronized void close() throws IOException {
        connected = false;

        if (inputStream != null) {
            try {
                inputStream.close();
            } catch (IOException e) {
                logger.warn("Error closing input stream", e);
            }
        }

        if (outputStream != null) {
            try {
                outputStream.close();
            } catch (IOException e) {
                logger.warn("Error closing output stream", e);
            }
        }

        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
                logger.warn("Error closing socket", e);
            }
        }

        logger.info("Connection closed");
    }
}
