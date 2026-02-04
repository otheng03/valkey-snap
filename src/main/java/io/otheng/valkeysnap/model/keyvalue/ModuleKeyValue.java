package io.otheng.valkeysnap.model.keyvalue;

import java.util.Arrays;

/**
 * KeyValue event for Redis MODULE type.
 */
public record ModuleKeyValue(
    byte[] key,
    String moduleName,
    long expireTimeMs,
    int db
) implements KeyValueEvent {

    public ModuleKeyValue {
        key = key != null ? key.clone() : new byte[0];
        moduleName = moduleName != null ? moduleName : "";
    }

    @Override
    public byte[] key() {
        return key.clone();
    }

    @Override
    public String typeName() {
        return "MODULE";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ModuleKeyValue that)) return false;
        return expireTimeMs == that.expireTimeMs &&
               db == that.db &&
               Arrays.equals(key, that.key) &&
               moduleName.equals(that.moduleName);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(key);
        result = 31 * result + moduleName.hashCode();
        result = 31 * result + Long.hashCode(expireTimeMs);
        result = 31 * result + db;
        return result;
    }

    @Override
    public String toString() {
        return "ModuleKeyValue{key=" + keyAsString() +
               ", moduleName=" + moduleName +
               ", expireTimeMs=" + expireTimeMs +
               ", db=" + db + "}";
    }
}
