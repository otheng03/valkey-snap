package io.otheng.valkeysnap.model.keyvalue;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * KeyValue event for Redis HASH type.
 */
public record HashKeyValue(
    byte[] key,
    List<HashField> fields,
    long expireTimeMs,
    int db
) implements KeyValueEvent {

    /**
     * A field-value pair in a hash.
     */
    public record HashField(byte[] field, byte[] value) {
        public HashField {
            field = field != null ? field.clone() : new byte[0];
            value = value != null ? value.clone() : new byte[0];
        }

        public byte[] field() {
            return field.clone();
        }

        public byte[] value() {
            return value.clone();
        }

        public String fieldAsString() {
            return new String(field, StandardCharsets.UTF_8);
        }

        public String valueAsString() {
            return new String(value, StandardCharsets.UTF_8);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof HashField that)) return false;
            return Arrays.equals(field, that.field) &&
                   Arrays.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            int result = Arrays.hashCode(field);
            result = 31 * result + Arrays.hashCode(value);
            return result;
        }

        @Override
        public String toString() {
            return fieldAsString() + "=" + valueAsString();
        }
    }

    public HashKeyValue {
        key = key != null ? key.clone() : new byte[0];
        fields = fields != null ? List.copyOf(fields) : List.of();
    }

    /**
     * Creates a HashKeyValue from a map of field bytes to value bytes.
     */
    public static HashKeyValue fromMap(byte[] key, Map<byte[], byte[]> map, long expireTimeMs, int db) {
        List<HashField> fields = new ArrayList<>(map.size());
        for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
            fields.add(new HashField(entry.getKey(), entry.getValue()));
        }
        return new HashKeyValue(key, fields, expireTimeMs, db);
    }

    @Override
    public byte[] key() {
        return key.clone();
    }

    /**
     * Returns the fields as a map from field string to value string.
     */
    public Map<String, String> fieldsAsStringMap() {
        Map<String, String> map = new LinkedHashMap<>();
        for (HashField field : fields) {
            map.put(field.fieldAsString(), field.valueAsString());
        }
        return map;
    }

    /**
     * Returns the number of fields in the hash.
     */
    public int size() {
        return fields.size();
    }

    @Override
    public String typeName() {
        return "HASH";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof HashKeyValue that)) return false;
        return expireTimeMs == that.expireTimeMs &&
               db == that.db &&
               Arrays.equals(key, that.key) &&
               fields.equals(that.fields);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(key);
        result = 31 * result + fields.hashCode();
        result = 31 * result + Long.hashCode(expireTimeMs);
        result = 31 * result + db;
        return result;
    }

    @Override
    public String toString() {
        return "HashKeyValue{key=" + keyAsString() +
               ", size=" + fields.size() +
               ", expireTimeMs=" + expireTimeMs +
               ", db=" + db + "}";
    }
}
