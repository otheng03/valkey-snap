package io.otheng.valkeysnap.protocol.resp;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Parser for the Redis Serialization Protocol (RESP).
 * Supports RESP2 protocol types: Simple String, Error, Integer, Bulk String, Array.
 */
public class RespParser {

    private static final int CR = '\r';
    private static final int LF = '\n';
    private static final int MAX_LINE_LENGTH = 64 * 1024; // 64KB max line

    private final InputStream in;
    private final byte[] lineBuffer = new byte[MAX_LINE_LENGTH];

    public RespParser(InputStream in) {
        this.in = in;
    }

    /**
     * Parses the next RESP value from the input stream.
     *
     * @return the parsed RespValue
     * @throws IOException if an I/O error occurs
     * @throws RespParseException if the data is malformed
     */
    public RespValue parse() throws IOException {
        int prefix = in.read();
        if (prefix < 0) {
            throw new EOFException("End of stream while reading RESP type");
        }

        RespType type = RespType.fromPrefix(prefix);

        return switch (type) {
            case SIMPLE_STRING -> parseSimpleString();
            case ERROR -> parseError();
            case INTEGER -> parseInteger();
            case BULK_STRING -> parseBulkString();
            case ARRAY -> parseArray();
        };
    }

    /**
     * Reads a line terminated by \r\n.
     *
     * @return the line content without the terminator
     */
    private String readLine() throws IOException {
        int pos = 0;
        int b;

        while ((b = in.read()) >= 0) {
            if (b == CR) {
                int next = in.read();
                if (next == LF) {
                    return new String(lineBuffer, 0, pos, StandardCharsets.UTF_8);
                }
                throw new RespParseException("Expected LF after CR");
            }

            if (pos >= MAX_LINE_LENGTH) {
                throw new RespParseException("Line too long (max " + MAX_LINE_LENGTH + " bytes)");
            }
            lineBuffer[pos++] = (byte) b;
        }

        throw new EOFException("Unexpected end of stream while reading line");
    }

    /**
     * Reads exactly n bytes from the input stream.
     */
    private byte[] readBytes(int n) throws IOException {
        byte[] buffer = new byte[n];
        int total = 0;
        while (total < n) {
            int read = in.read(buffer, total, n - total);
            if (read < 0) {
                throw new EOFException("Expected " + n + " bytes, got " + total);
            }
            total += read;
        }
        return buffer;
    }

    /**
     * Reads and discards \r\n terminator.
     */
    private void readCrlf() throws IOException {
        int cr = in.read();
        int lf = in.read();
        if (cr != CR || lf != LF) {
            throw new RespParseException("Expected CRLF terminator");
        }
    }

    private RespValue.SimpleString parseSimpleString() throws IOException {
        return new RespValue.SimpleString(readLine());
    }

    private RespValue.Error parseError() throws IOException {
        return new RespValue.Error(readLine());
    }

    private RespValue.Integer parseInteger() throws IOException {
        String line = readLine();
        try {
            return new RespValue.Integer(Long.parseLong(line));
        } catch (NumberFormatException e) {
            throw new RespParseException("Invalid integer: " + line, e);
        }
    }

    private RespValue.BulkString parseBulkString() throws IOException {
        String line = readLine();
        int length;
        try {
            length = Integer.parseInt(line);
        } catch (NumberFormatException e) {
            throw new RespParseException("Invalid bulk string length: " + line, e);
        }

        // Null bulk string ($-1\r\n)
        if (length < 0) {
            return RespValue.nullBulkString();
        }

        byte[] data = readBytes(length);
        readCrlf();
        return new RespValue.BulkString(data);
    }

    private RespValue.Array parseArray() throws IOException {
        String line = readLine();
        int count;
        try {
            count = Integer.parseInt(line);
        } catch (NumberFormatException e) {
            throw new RespParseException("Invalid array count: " + line, e);
        }

        // Null array (*-1\r\n)
        if (count < 0) {
            return RespValue.nullArray();
        }

        List<RespValue> elements = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            elements.add(parse());
        }

        return new RespValue.Array(elements);
    }

    /**
     * Exception thrown when RESP data is malformed.
     */
    public static class RespParseException extends IOException {
        public RespParseException(String message) {
            super(message);
        }

        public RespParseException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
