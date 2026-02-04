package io.otheng.valkeysnap.model.keyvalue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * KeyValue event for Redis STRING type.
 */
public record StringKeyValue(
    byte[] key,
    byte[] value,
    long expireTimeMs,
    int db
) implements KeyValueEvent {

    public StringKeyValue {
        key = key != null ? key.clone() : new byte[0];
        value = value != null ? value.clone() : new byte[0];
    }

    @Override
    public byte[] key() {
        return key.clone();
    }

    public byte[] value() {
        return value.clone();
    }

    /**
     * Returns the value as a UTF-8 string.
     */
    public String valueAsString() {
        return new String(value, StandardCharsets.UTF_8);
    }

    @Override
    public String typeName() {
        return "STRING";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StringKeyValue that)) return false;
        return expireTimeMs == that.expireTimeMs &&
               db == that.db &&
               Arrays.equals(key, that.key) &&
               Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        result = 31 * result + Long.hashCode(expireTimeMs);
        result = 31 * result + db;
        return result;
    }

    @Override
    public String toString() {
        return "StringKeyValue{key=" + keyAsString() +
               ", valueLen=" + value.length +
               ", expireTimeMs=" + expireTimeMs +
               ", db=" + db + "}";
    }
}
