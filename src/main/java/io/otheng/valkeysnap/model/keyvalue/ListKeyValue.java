package io.otheng.valkeysnap.model.keyvalue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * KeyValue event for Redis LIST type.
 */
public record ListKeyValue(
    byte[] key,
    List<byte[]> values,
    long expireTimeMs,
    int db
) implements KeyValueEvent {

    public ListKeyValue {
        key = key != null ? key.clone() : new byte[0];
        values = values != null ? copyByteList(values) : List.of();
    }

    private static List<byte[]> copyByteList(List<byte[]> original) {
        List<byte[]> copy = new ArrayList<>(original.size());
        for (byte[] item : original) {
            copy.add(item != null ? item.clone() : new byte[0]);
        }
        return List.copyOf(copy);
    }

    @Override
    public byte[] key() {
        return key.clone();
    }

    public List<byte[]> values() {
        return copyByteList(values);
    }

    /**
     * Returns the values as UTF-8 strings.
     */
    public List<String> valuesAsStrings() {
        return values.stream()
            .map(v -> new String(v, StandardCharsets.UTF_8))
            .toList();
    }

    /**
     * Returns the number of elements in the list.
     */
    public int size() {
        return values.size();
    }

    @Override
    public String typeName() {
        return "LIST";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ListKeyValue that)) return false;
        if (expireTimeMs != that.expireTimeMs || db != that.db) return false;
        if (!Arrays.equals(key, that.key)) return false;
        if (values.size() != that.values.size()) return false;
        for (int i = 0; i < values.size(); i++) {
            if (!Arrays.equals(values.get(i), that.values.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(key);
        for (byte[] value : values) {
            result = 31 * result + Arrays.hashCode(value);
        }
        result = 31 * result + Long.hashCode(expireTimeMs);
        result = 31 * result + db;
        return result;
    }

    @Override
    public String toString() {
        return "ListKeyValue{key=" + keyAsString() +
               ", size=" + values.size() +
               ", expireTimeMs=" + expireTimeMs +
               ", db=" + db + "}";
    }
}
