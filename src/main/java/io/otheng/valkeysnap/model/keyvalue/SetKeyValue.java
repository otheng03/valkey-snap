package io.otheng.valkeysnap.model.keyvalue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * KeyValue event for Redis SET type.
 */
public record SetKeyValue(
    byte[] key,
    List<byte[]> members,
    long expireTimeMs,
    int db
) implements KeyValueEvent {

    public SetKeyValue {
        key = key != null ? key.clone() : new byte[0];
        members = members != null ? copyByteList(members) : List.of();
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

    public List<byte[]> members() {
        return copyByteList(members);
    }

    /**
     * Returns the members as UTF-8 strings.
     */
    public Set<String> membersAsStrings() {
        return members.stream()
            .map(m -> new String(m, StandardCharsets.UTF_8))
            .collect(Collectors.toSet());
    }

    /**
     * Returns the number of members in the set.
     */
    public int size() {
        return members.size();
    }

    @Override
    public String typeName() {
        return "SET";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SetKeyValue that)) return false;
        if (expireTimeMs != that.expireTimeMs || db != that.db) return false;
        if (!Arrays.equals(key, that.key)) return false;
        if (members.size() != that.members.size()) return false;
        for (int i = 0; i < members.size(); i++) {
            if (!Arrays.equals(members.get(i), that.members.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(key);
        for (byte[] member : members) {
            result = 31 * result + Arrays.hashCode(member);
        }
        result = 31 * result + Long.hashCode(expireTimeMs);
        result = 31 * result + db;
        return result;
    }

    @Override
    public String toString() {
        return "SetKeyValue{key=" + keyAsString() +
               ", size=" + members.size() +
               ", expireTimeMs=" + expireTimeMs +
               ", db=" + db + "}";
    }
}
