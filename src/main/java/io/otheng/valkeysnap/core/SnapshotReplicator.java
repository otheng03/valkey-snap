package io.otheng.valkeysnap.core;

import io.otheng.valkeysnap.config.SnapshotConfig;
import io.otheng.valkeysnap.consumer.SnapshotListener;
import io.otheng.valkeysnap.consumer.events.*;
import io.otheng.valkeysnap.model.command.ChunkingStrategy;
import io.otheng.valkeysnap.model.command.CommandSplitter;
import io.otheng.valkeysnap.model.keyvalue.*;
import io.otheng.valkeysnap.model.raw.RawBytesEvent;
import io.otheng.valkeysnap.network.ReplicationHandshake;
import io.otheng.valkeysnap.network.ValkeyConnection;
import io.otheng.valkeysnap.protocol.rdb.RdbInputStream;
import io.otheng.valkeysnap.protocol.rdb.RdbParser;
import io.otheng.valkeysnap.protocol.rdb.RdbVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Core replication engine that orchestrates the PSYNC flow.
 *
 * <p>Coordinates:
 * <ul>
 *   <li>Network connection and handshake</li>
 *   <li>RDB stream reading and parsing</li>
 *   <li>Event emission to listeners</li>
 *   <li>Optional command splitting</li>
 * </ul>
 */
public class SnapshotReplicator implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(SnapshotReplicator.class);

    private final SnapshotConfig config;
    private final SnapshotListener listener;
    private final CommandSplitter commandSplitter;

    private ValkeyConnection connection;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong keyCount = new AtomicLong(0);
    private volatile int currentDb = 0;

    public SnapshotReplicator(SnapshotConfig config, SnapshotListener listener) {
        this.config = config;
        this.listener = listener;
        this.commandSplitter = new CommandSplitter(
            new ChunkingStrategy(config.commandChunkMaxElements(), config.commandChunkMaxBytes())
        );
    }

    /**
     * Starts the replication process.
     * Blocks until replication completes or an error occurs.
     *
     * @throws IOException if connection or replication fails
     */
    public void start() throws IOException {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("Replicator is already running");
        }

        try {
            // Connect
            connection = new ValkeyConnection(config);
            connection.connect();

            // Handshake
            ReplicationHandshake handshake = new ReplicationHandshake(connection, config);
            handshake.performHandshake();

            listener.onPsyncStart(new PsyncStartEvent(
                handshake.getMasterReplId(),
                handshake.getMasterReplOffset(),
                handshake.isFullSync()
            ));

            // Read RDB if full sync
            if (handshake.isFullSync()) {
                readRdb();
            }

            listener.onPsyncEnd(new PsyncEndEvent("completed"));

        } catch (Exception e) {
            listener.onError(e);
            listener.onPsyncEnd(new PsyncEndEvent("error: " + e.getMessage()));
            throw e;
        } finally {
            running.set(false);
            close();
        }
    }

    private void readRdb() throws IOException {
        InputStream in = connection.getInputStream();

        // Read RDB size prefix ($<size>\r\n)
        long rdbSize = readRdbSizePrefix(in);
        logger.info("RDB size: {} bytes", rdbSize);

        // Create RDB input stream (optionally with size limit)
        InputStream rdbStream;
        if (rdbSize > 0) {
            rdbStream = new BoundedInputStream(in, rdbSize);
        } else {
            // EOF-based termination
            rdbStream = in;
        }

        // Handle raw bytes mode
        if (config.emitRawBytes()) {
            readRdbRaw(rdbStream, rdbSize);
        } else {
            parseRdb(rdbStream);
        }
    }

    private long readRdbSizePrefix(InputStream in) throws IOException {
        int b = in.read();
        if (b != '$') {
            throw new IOException("Expected '$' prefix for RDB, got: " + (char) b);
        }

        StringBuilder sb = new StringBuilder();
        while (true) {
            b = in.read();
            if (b < 0) {
                throw new EOFException("Unexpected end of stream reading RDB size");
            }
            if (b == '\r') {
                b = in.read();
                if (b != '\n') {
                    throw new IOException("Expected LF after CR in RDB size");
                }
                break;
            }
            sb.append((char) b);
        }

        String sizeStr = sb.toString();
        if ("EOF".equals(sizeStr) || sizeStr.startsWith("EOF:")) {
            // Diskless replication with EOF marker
            return -1;
        }

        return Long.parseLong(sizeStr);
    }

    private void readRdbRaw(InputStream rdbStream, long rdbSize) throws IOException {
        byte[] buffer = new byte[64 * 1024];
        long offset = 0;
        int read;

        while ((read = rdbStream.read(buffer)) > 0) {
            byte[] data = new byte[read];
            System.arraycopy(buffer, 0, data, 0, read);
            listener.onRawBytes(new RawBytesEvent(data, offset));
            offset += read;
        }

        listener.onSnapshotEnd(new SnapshotEndEvent(new byte[8], 0));
    }

    private void parseRdb(InputStream rdbStream) throws IOException {
        RdbInputStream rdbIn = new RdbInputStream(rdbStream);
        RdbParser parser = new RdbParser(rdbIn, new RdbVisitorImpl());
        parser.parse();
    }

    /**
     * Stops the replication process.
     */
    public void stop() {
        running.set(false);
        try {
            close();
        } catch (IOException e) {
            logger.warn("Error closing connection during stop", e);
        }
    }

    /**
     * Returns true if replication is currently running.
     */
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public void close() throws IOException {
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }

    /**
     * RDB visitor that emits events to the listener.
     */
    private class RdbVisitorImpl implements RdbVisitor {

        @Override
        public void onRdbStart(int version) {
            listener.onSnapshotBegin(new SnapshotBeginEvent(version));
        }

        @Override
        public void onSelectDb(int dbNumber) {
            currentDb = dbNumber;
            listener.onDbSelect(new DbSelectEvent(dbNumber));
        }

        @Override
        public void onString(byte[] key, byte[] value, long expireTimeMs) {
            keyCount.incrementAndGet();
            StringKeyValue kv = new StringKeyValue(key, value, expireTimeMs, currentDb);
            emitKeyValue(kv);
        }

        @Override
        public void onList(byte[] key, List<byte[]> values, long expireTimeMs) {
            keyCount.incrementAndGet();
            ListKeyValue kv = new ListKeyValue(key, values, expireTimeMs, currentDb);
            emitKeyValue(kv);
        }

        @Override
        public void onSet(byte[] key, List<byte[]> members, long expireTimeMs) {
            keyCount.incrementAndGet();
            SetKeyValue kv = new SetKeyValue(key, members, expireTimeMs, currentDb);
            emitKeyValue(kv);
        }

        @Override
        public void onSortedSet(byte[] key, Map<byte[], Double> entries, long expireTimeMs) {
            keyCount.incrementAndGet();
            SortedSetKeyValue kv = SortedSetKeyValue.fromMap(key, entries, expireTimeMs, currentDb);
            emitKeyValue(kv);
        }

        @Override
        public void onHash(byte[] key, Map<byte[], byte[]> fields, long expireTimeMs) {
            keyCount.incrementAndGet();
            HashKeyValue kv = HashKeyValue.fromMap(key, fields, expireTimeMs, currentDb);
            emitKeyValue(kv);
        }

        @Override
        public void onStream(byte[] key, long expireTimeMs) {
            keyCount.incrementAndGet();
            StreamKeyValue kv = new StreamKeyValue(key, expireTimeMs, currentDb);
            emitKeyValue(kv);
        }

        @Override
        public void onModule(byte[] key, String moduleName, long expireTimeMs) {
            keyCount.incrementAndGet();
            ModuleKeyValue kv = new ModuleKeyValue(key, moduleName, expireTimeMs, currentDb);
            emitKeyValue(kv);
        }

        @Override
        public void onRdbEnd(byte[] checksum) {
            listener.onSnapshotEnd(new SnapshotEndEvent(checksum, keyCount.get()));
        }

        @Override
        public void onError(Exception error) {
            listener.onError(error);
        }

        private void emitKeyValue(KeyValueEvent kv) {
            if (config.emitKeyValueEvents()) {
                listener.onKeyValue(kv);
            }

            if (config.emitCommandEvents()) {
                commandSplitter.split(kv, listener::onCommand);
            }
        }
    }

    /**
     * InputStream wrapper that limits reads to a fixed number of bytes.
     */
    private static class BoundedInputStream extends FilterInputStream {
        private long remaining;

        public BoundedInputStream(InputStream in, long limit) {
            super(in);
            this.remaining = limit;
        }

        @Override
        public int read() throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            int b = super.read();
            if (b >= 0) {
                remaining--;
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            int toRead = (int) Math.min(len, remaining);
            int n = super.read(b, off, toRead);
            if (n > 0) {
                remaining -= n;
            }
            return n;
        }

        @Override
        public long skip(long n) throws IOException {
            long toSkip = Math.min(n, remaining);
            long skipped = super.skip(toSkip);
            remaining -= skipped;
            return skipped;
        }

        @Override
        public int available() throws IOException {
            return (int) Math.min(super.available(), remaining);
        }
    }
}
