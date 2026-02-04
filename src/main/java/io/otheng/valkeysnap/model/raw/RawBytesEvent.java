package io.otheng.valkeysnap.model.raw;

import java.util.Arrays;

/**
 * Event containing raw bytes from the replication stream.
 * Used in pass-through mode where no parsing is performed.
 *
 * <p>Note: In pass-through mode, byte boundaries do not align with
 * semantic boundaries (keys or commands). The bytes are delivered
 * as received from the network stream.
 *
 * @param data the raw bytes
 * @param offset the offset in the replication stream
 */
public record RawBytesEvent(byte[] data, long offset) {

    public RawBytesEvent {
        data = data != null ? data.clone() : new byte[0];
    }

    public byte[] data() {
        return data.clone();
    }

    /**
     * Returns the length of the data.
     */
    public int length() {
        return data.length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RawBytesEvent that)) return false;
        return offset == that.offset && Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(data);
        result = 31 * result + Long.hashCode(offset);
        return result;
    }

    @Override
    public String toString() {
        return "RawBytesEvent{length=" + data.length + ", offset=" + offset + "}";
    }
}
