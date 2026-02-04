package io.otheng.valkeysnap.util;

/**
 * CRC-64 implementation compatible with Redis RDB checksums.
 * Uses the ECMA-182 polynomial.
 */
public final class CRC64 {

    private static final long POLY = 0xC96C5795D7870F42L;
    private static final long[] TABLE = new long[256];

    static {
        for (int i = 0; i < 256; i++) {
            long crc = i;
            for (int j = 0; j < 8; j++) {
                if ((crc & 1) == 1) {
                    crc = (crc >>> 1) ^ POLY;
                } else {
                    crc >>>= 1;
                }
            }
            TABLE[i] = crc;
        }
    }

    private long crc;

    public CRC64() {
        this.crc = 0;
    }

    /**
     * Updates the CRC with a single byte.
     */
    public void update(byte b) {
        crc = TABLE[(int) ((crc ^ b) & 0xFF)] ^ (crc >>> 8);
    }

    /**
     * Updates the CRC with a byte array.
     */
    public void update(byte[] bytes) {
        update(bytes, 0, bytes.length);
    }

    /**
     * Updates the CRC with a portion of a byte array.
     */
    public void update(byte[] bytes, int offset, int length) {
        for (int i = 0; i < length; i++) {
            update(bytes[offset + i]);
        }
    }

    /**
     * Returns the current CRC value.
     */
    public long getValue() {
        return crc;
    }

    /**
     * Resets the CRC to its initial state.
     */
    public void reset() {
        crc = 0;
    }

    /**
     * Returns the CRC as an 8-byte array (little-endian).
     */
    public byte[] toBytes() {
        byte[] result = new byte[8];
        long value = crc;
        for (int i = 0; i < 8; i++) {
            result[i] = (byte) (value & 0xFF);
            value >>>= 8;
        }
        return result;
    }

    /**
     * Computes CRC-64 of a byte array.
     */
    public static long compute(byte[] bytes) {
        CRC64 crc64 = new CRC64();
        crc64.update(bytes);
        return crc64.getValue();
    }

    /**
     * Computes CRC-64 and returns as bytes.
     */
    public static byte[] computeBytes(byte[] bytes) {
        CRC64 crc64 = new CRC64();
        crc64.update(bytes);
        return crc64.toBytes();
    }
}
