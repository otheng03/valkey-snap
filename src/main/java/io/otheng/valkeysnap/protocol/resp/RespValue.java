package io.otheng.valkeysnap.protocol.resp;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Represents a parsed RESP value.
 */
public sealed interface RespValue {

    /**
     * Returns the RESP type of this value.
     */
    RespType type();

    /**
     * Simple String value (e.g., +OK).
     */
    record SimpleString(String value) implements RespValue {
        @Override
        public RespType type() {
            return RespType.SIMPLE_STRING;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    /**
     * Error value (e.g., -ERR message).
     */
    record Error(String message) implements RespValue {
        @Override
        public RespType type() {
            return RespType.ERROR;
        }

        @Override
        public String toString() {
            return "ERROR: " + message;
        }
    }

    /**
     * Integer value (e.g., :1000).
     */
    record Integer(long value) implements RespValue {
        @Override
        public RespType type() {
            return RespType.INTEGER;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }
    }

    /**
     * Bulk String value. Can be null (nil bulk string).
     */
    record BulkString(byte[] data) implements RespValue {
        public BulkString {
            // Defensive copy for immutability
            data = data != null ? data.clone() : null;
        }

        @Override
        public RespType type() {
            return RespType.BULK_STRING;
        }

        /**
         * Returns true if this is a nil bulk string.
         */
        public boolean isNull() {
            return data == null;
        }

        /**
         * Returns the data as a UTF-8 string.
         */
        public String asString() {
            return data != null ? new String(data, StandardCharsets.UTF_8) : null;
        }

        /**
         * Returns the raw byte data.
         */
        @Override
        public byte[] data() {
            return data != null ? data.clone() : null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof BulkString that)) return false;
            return Arrays.equals(data, that.data);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(data);
        }

        @Override
        public String toString() {
            if (data == null) return "(nil)";
            if (data.length > 100) {
                return new String(data, 0, 100, StandardCharsets.UTF_8) + "...(" + data.length + " bytes)";
            }
            return new String(data, StandardCharsets.UTF_8);
        }
    }

    /**
     * Array value. Can be null (nil array).
     */
    record Array(List<RespValue> elements) implements RespValue {
        public Array {
            // Defensive copy for immutability
            elements = elements != null ? List.copyOf(elements) : null;
        }

        @Override
        public RespType type() {
            return RespType.ARRAY;
        }

        /**
         * Returns true if this is a nil array.
         */
        public boolean isNull() {
            return elements == null;
        }

        /**
         * Returns the number of elements, or -1 if null.
         */
        public int size() {
            return elements != null ? elements.size() : -1;
        }

        /**
         * Gets the element at the specified index.
         */
        public RespValue get(int index) {
            Objects.requireNonNull(elements, "Array is null");
            return elements.get(index);
        }

        @Override
        public String toString() {
            if (elements == null) return "(nil array)";
            return elements.toString();
        }
    }

    // Utility methods

    /**
     * Creates a simple string value.
     */
    static SimpleString simpleString(String value) {
        return new SimpleString(value);
    }

    /**
     * Creates an error value.
     */
    static Error error(String message) {
        return new Error(message);
    }

    /**
     * Creates an integer value.
     */
    static Integer integer(long value) {
        return new Integer(value);
    }

    /**
     * Creates a bulk string value from bytes.
     */
    static BulkString bulkString(byte[] data) {
        return new BulkString(data);
    }

    /**
     * Creates a bulk string value from a string.
     */
    static BulkString bulkString(String value) {
        return new BulkString(value != null ? value.getBytes(StandardCharsets.UTF_8) : null);
    }

    /**
     * Creates a nil bulk string.
     */
    static BulkString nullBulkString() {
        return new BulkString((byte[]) null);
    }

    /**
     * Creates an array value.
     */
    static Array array(List<RespValue> elements) {
        return new Array(elements);
    }

    /**
     * Creates an array value from varargs.
     */
    static Array array(RespValue... elements) {
        return new Array(Arrays.asList(elements));
    }

    /**
     * Creates a nil array.
     */
    static Array nullArray() {
        return new Array((List<RespValue>) null);
    }
}
