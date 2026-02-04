package io.otheng.valkeysnap.model.keyvalue;

import java.nio.charset.StandardCharsets;

/**
 * Base sealed interface for all key-value events from RDB parsing.
 * Each concrete type represents a different Redis data type.
 */
public sealed interface KeyValueEvent
    permits StringKeyValue, ListKeyValue, SetKeyValue, SortedSetKeyValue, HashKeyValue, StreamKeyValue, ModuleKeyValue {

    /**
     * Returns the key as raw bytes.
     */
    byte[] key();

    /**
     * Returns the expiration time in milliseconds since epoch, or -1 if no expiration.
     */
    long expireTimeMs();

    /**
     * Returns the database number this key belongs to.
     */
    int db();

    /**
     * Returns the key as a UTF-8 string.
     */
    default String keyAsString() {
        return new String(key(), StandardCharsets.UTF_8);
    }

    /**
     * Returns true if this key has an expiration time set.
     */
    default boolean hasExpiration() {
        return expireTimeMs() > 0;
    }

    /**
     * Returns the type name of this event (STRING, LIST, SET, ZSET, HASH, STREAM, MODULE).
     */
    String typeName();
}
