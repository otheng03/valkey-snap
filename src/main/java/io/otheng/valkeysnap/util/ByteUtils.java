package io.otheng.valkeysnap.util;

import java.nio.charset.StandardCharsets;

/**
 * Utility methods for byte array operations.
 */
public final class ByteUtils {

    private ByteUtils() {}

    /**
     * Converts a byte array to a UTF-8 string.
     */
    public static String toString(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Converts a string to a UTF-8 byte array.
     */
    public static byte[] toBytes(String str) {
        if (str == null) {
            return null;
        }
        return str.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Compares two byte arrays for equality.
     */
    public static boolean equals(byte[] a, byte[] b) {
        if (a == b) return true;
        if (a == null || b == null) return false;
        if (a.length != b.length) return false;
        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i]) return false;
        }
        return true;
    }

    /**
     * Copies a byte array.
     */
    public static byte[] copy(byte[] src) {
        if (src == null) return null;
        byte[] copy = new byte[src.length];
        System.arraycopy(src, 0, copy, 0, src.length);
        return copy;
    }

    /**
     * Converts a byte array to a hexadecimal string.
     */
    public static String toHex(byte[] bytes) {
        if (bytes == null) return null;
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xFF));
        }
        return sb.toString();
    }

    /**
     * Parses a hexadecimal string to a byte array.
     */
    public static byte[] fromHex(String hex) {
        if (hex == null) return null;
        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                                 + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
    }

    /**
     * Reads a little-endian 16-bit unsigned integer from bytes.
     */
    public static int readUint16LE(byte[] bytes, int offset) {
        return (bytes[offset] & 0xFF) | ((bytes[offset + 1] & 0xFF) << 8);
    }

    /**
     * Reads a little-endian 32-bit unsigned integer from bytes.
     */
    public static long readUint32LE(byte[] bytes, int offset) {
        return ((long) (bytes[offset] & 0xFF)) |
               ((long) (bytes[offset + 1] & 0xFF) << 8) |
               ((long) (bytes[offset + 2] & 0xFF) << 16) |
               ((long) (bytes[offset + 3] & 0xFF) << 24);
    }

    /**
     * Reads a little-endian 64-bit signed integer from bytes.
     */
    public static long readInt64LE(byte[] bytes, int offset) {
        return ((long) (bytes[offset] & 0xFF)) |
               ((long) (bytes[offset + 1] & 0xFF) << 8) |
               ((long) (bytes[offset + 2] & 0xFF) << 16) |
               ((long) (bytes[offset + 3] & 0xFF) << 24) |
               ((long) (bytes[offset + 4] & 0xFF) << 32) |
               ((long) (bytes[offset + 5] & 0xFF) << 40) |
               ((long) (bytes[offset + 6] & 0xFF) << 48) |
               ((long) (bytes[offset + 7] & 0xFF) << 56);
    }
}
