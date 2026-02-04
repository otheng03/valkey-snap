package io.otheng.valkeysnap.protocol.rdb;

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * Buffered binary input stream optimized for RDB parsing.
 * Provides methods for reading various data types and tracking read offset.
 */
public class RdbInputStream extends FilterInputStream {

    private long offset;
    private final byte[] tempBuffer = new byte[8];

    public RdbInputStream(InputStream in) {
        super(in);
        this.offset = 0;
    }

    /**
     * Returns the current byte offset (total bytes read).
     */
    public long getOffset() {
        return offset;
    }

    @Override
    public int read() throws IOException {
        int b = super.read();
        if (b >= 0) {
            offset++;
        }
        return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int n = super.read(b, off, len);
        if (n > 0) {
            offset += n;
        }
        return n;
    }

    /**
     * Reads a single byte, throwing EOFException if end of stream.
     */
    public int readByte() throws IOException {
        int b = read();
        if (b < 0) {
            throw new EOFException("Unexpected end of stream");
        }
        return b;
    }

    /**
     * Reads exactly the specified number of bytes into a new array.
     *
     * @param length number of bytes to read
     * @return byte array containing the read bytes
     * @throws EOFException if end of stream reached before reading all bytes
     */
    public byte[] readBytes(int length) throws IOException {
        byte[] buffer = new byte[length];
        readFully(buffer);
        return buffer;
    }

    /**
     * Reads exactly buffer.length bytes into the buffer.
     *
     * @throws EOFException if end of stream reached before filling buffer
     */
    public void readFully(byte[] buffer) throws IOException {
        readFully(buffer, 0, buffer.length);
    }

    /**
     * Reads exactly len bytes into the buffer starting at offset.
     *
     * @throws EOFException if end of stream reached before reading all bytes
     */
    public void readFully(byte[] buffer, int off, int len) throws IOException {
        int total = 0;
        while (total < len) {
            int n = read(buffer, off + total, len - total);
            if (n < 0) {
                throw new EOFException("Expected " + len + " bytes, got " + total);
            }
            total += n;
        }
    }

    /**
     * Reads an unsigned 8-bit integer.
     */
    public int readUint8() throws IOException {
        return readByte() & 0xFF;
    }

    /**
     * Reads a signed 8-bit integer.
     */
    public int readInt8() throws IOException {
        return (byte) readByte();
    }

    /**
     * Reads an unsigned 16-bit little-endian integer.
     */
    public int readUint16LE() throws IOException {
        readFully(tempBuffer, 0, 2);
        return (tempBuffer[0] & 0xFF) | ((tempBuffer[1] & 0xFF) << 8);
    }

    /**
     * Reads a signed 16-bit little-endian integer.
     */
    public int readInt16LE() throws IOException {
        return (short) readUint16LE();
    }

    /**
     * Reads an unsigned 32-bit little-endian integer.
     */
    public long readUint32LE() throws IOException {
        readFully(tempBuffer, 0, 4);
        return ((long) (tempBuffer[0] & 0xFF)) |
               ((long) (tempBuffer[1] & 0xFF) << 8) |
               ((long) (tempBuffer[2] & 0xFF) << 16) |
               ((long) (tempBuffer[3] & 0xFF) << 24);
    }

    /**
     * Reads a signed 32-bit little-endian integer.
     */
    public int readInt32LE() throws IOException {
        return (int) readUint32LE();
    }

    /**
     * Reads a signed 64-bit little-endian integer.
     */
    public long readInt64LE() throws IOException {
        readFully(tempBuffer, 0, 8);
        return ByteBuffer.wrap(tempBuffer).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    /**
     * Reads an unsigned 32-bit big-endian integer.
     */
    public long readUint32BE() throws IOException {
        readFully(tempBuffer, 0, 4);
        return ((long) (tempBuffer[0] & 0xFF) << 24) |
               ((long) (tempBuffer[1] & 0xFF) << 16) |
               ((long) (tempBuffer[2] & 0xFF) << 8) |
               ((long) (tempBuffer[3] & 0xFF));
    }

    /**
     * Reads a double value (8 bytes, little-endian).
     */
    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readInt64LE());
    }

    /**
     * Reads bytes as a UTF-8 string.
     *
     * @param length number of bytes to read
     * @return the decoded string
     */
    public String readString(int length) throws IOException {
        byte[] bytes = readBytes(length);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Skips exactly the specified number of bytes.
     *
     * @throws EOFException if end of stream reached before skipping all bytes
     */
    public void skipBytes(long n) throws IOException {
        long remaining = n;
        while (remaining > 0) {
            long skipped = skip(remaining);
            if (skipped <= 0) {
                // skip() might return 0, try reading instead
                int b = read();
                if (b < 0) {
                    throw new EOFException("Expected to skip " + n + " bytes, only skipped " + (n - remaining));
                }
                remaining--;
            } else {
                remaining -= skipped;
            }
        }
    }

    @Override
    public long skip(long n) throws IOException {
        long skipped = super.skip(n);
        if (skipped > 0) {
            offset += skipped;
        }
        return skipped;
    }
}
