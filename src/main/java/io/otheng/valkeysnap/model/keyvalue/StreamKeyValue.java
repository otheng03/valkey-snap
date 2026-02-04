package io.otheng.valkeysnap.model.keyvalue;

import java.util.Arrays;

/**
 * KeyValue event for Redis STREAM type.
 * Note: Full stream parsing is complex; this provides basic notification.
 */
public record StreamKeyValue(
    byte[] key,
    long expireTimeMs,
    int db
) implements KeyValueEvent {

    public StreamKeyValue {
        key = key != null ? key.clone() : new byte[0];
    }

    @Override
    public byte[] key() {
        return key.clone();
    }

    @Override
    public String typeName() {
        return "STREAM";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StreamKeyValue that)) return false;
        return expireTimeMs == that.expireTimeMs &&
               db == that.db &&
               Arrays.equals(key, that.key);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(key);
        result = 31 * result + Long.hashCode(expireTimeMs);
        result = 31 * result + db;
        return result;
    }

    @Override
    public String toString() {
        return "StreamKeyValue{key=" + keyAsString() +
               ", expireTimeMs=" + expireTimeMs +
               ", db=" + db + "}";
    }
}
