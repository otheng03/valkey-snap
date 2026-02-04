package io.otheng.valkeysnap.model.keyvalue;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * KeyValue event for Redis ZSET (Sorted Set) type.
 */
public record SortedSetKeyValue(
    byte[] key,
    List<ScoredMember> entries,
    long expireTimeMs,
    int db
) implements KeyValueEvent {

    /**
     * A member with its score in a sorted set.
     */
    public record ScoredMember(byte[] member, double score) {
        public ScoredMember {
            member = member != null ? member.clone() : new byte[0];
        }

        public byte[] member() {
            return member.clone();
        }

        public String memberAsString() {
            return new String(member, StandardCharsets.UTF_8);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ScoredMember that)) return false;
            return Double.compare(that.score, score) == 0 &&
                   Arrays.equals(member, that.member);
        }

        @Override
        public int hashCode() {
            int result = Arrays.hashCode(member);
            result = 31 * result + Double.hashCode(score);
            return result;
        }

        @Override
        public String toString() {
            return memberAsString() + ":" + score;
        }
    }

    public SortedSetKeyValue {
        key = key != null ? key.clone() : new byte[0];
        entries = entries != null ? List.copyOf(entries) : List.of();
    }

    /**
     * Creates a SortedSetKeyValue from a map of member bytes to scores.
     */
    public static SortedSetKeyValue fromMap(byte[] key, Map<byte[], Double> map, long expireTimeMs, int db) {
        List<ScoredMember> entries = new ArrayList<>(map.size());
        for (Map.Entry<byte[], Double> entry : map.entrySet()) {
            entries.add(new ScoredMember(entry.getKey(), entry.getValue()));
        }
        return new SortedSetKeyValue(key, entries, expireTimeMs, db);
    }

    @Override
    public byte[] key() {
        return key.clone();
    }

    /**
     * Returns the entries as a map from member string to score.
     */
    public Map<String, Double> entriesAsStringMap() {
        Map<String, Double> map = new LinkedHashMap<>();
        for (ScoredMember entry : entries) {
            map.put(entry.memberAsString(), entry.score());
        }
        return map;
    }

    /**
     * Returns the number of members in the sorted set.
     */
    public int size() {
        return entries.size();
    }

    @Override
    public String typeName() {
        return "ZSET";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SortedSetKeyValue that)) return false;
        return expireTimeMs == that.expireTimeMs &&
               db == that.db &&
               Arrays.equals(key, that.key) &&
               entries.equals(that.entries);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(key);
        result = 31 * result + entries.hashCode();
        result = 31 * result + Long.hashCode(expireTimeMs);
        result = 31 * result + db;
        return result;
    }

    @Override
    public String toString() {
        return "SortedSetKeyValue{key=" + keyAsString() +
               ", size=" + entries.size() +
               ", expireTimeMs=" + expireTimeMs +
               ", db=" + db + "}";
    }
}
