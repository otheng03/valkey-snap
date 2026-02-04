package io.otheng.valkeysnap.protocol.rdb;

import java.util.List;
import java.util.Map;

/**
 * Visitor interface for RDB parsing events.
 * Implement this interface to receive callbacks during RDB parsing.
 */
public interface RdbVisitor {

    /**
     * Called when RDB parsing starts.
     *
     * @param version the RDB version number
     */
    default void onRdbStart(int version) {}

    /**
     * Called for each auxiliary field in the RDB.
     *
     * @param key the auxiliary field name
     * @param value the auxiliary field value
     */
    default void onAuxField(String key, String value) {}

    /**
     * Called when a database is selected.
     *
     * @param dbNumber the database number
     */
    default void onSelectDb(int dbNumber) {}

    /**
     * Called when database resize info is encountered.
     *
     * @param dbSize the number of keys in the database
     * @param expiresSize the number of keys with expiration
     */
    default void onResizeDb(long dbSize, long expiresSize) {}

    /**
     * Called when a string key-value is parsed.
     *
     * @param key the key
     * @param value the string value
     * @param expireTimeMs expiration time in milliseconds since epoch, or -1 if no expiration
     */
    default void onString(byte[] key, byte[] value, long expireTimeMs) {}

    /**
     * Called when a list key-value is parsed.
     *
     * @param key the key
     * @param values the list values
     * @param expireTimeMs expiration time in milliseconds since epoch, or -1 if no expiration
     */
    default void onList(byte[] key, List<byte[]> values, long expireTimeMs) {}

    /**
     * Called when a set key-value is parsed.
     *
     * @param key the key
     * @param members the set members
     * @param expireTimeMs expiration time in milliseconds since epoch, or -1 if no expiration
     */
    default void onSet(byte[] key, List<byte[]> members, long expireTimeMs) {}

    /**
     * Called when a sorted set key-value is parsed.
     *
     * @param key the key
     * @param entries map of member to score
     * @param expireTimeMs expiration time in milliseconds since epoch, or -1 if no expiration
     */
    default void onSortedSet(byte[] key, Map<byte[], Double> entries, long expireTimeMs) {}

    /**
     * Called when a hash key-value is parsed.
     *
     * @param key the key
     * @param fields map of field to value
     * @param expireTimeMs expiration time in milliseconds since epoch, or -1 if no expiration
     */
    default void onHash(byte[] key, Map<byte[], byte[]> fields, long expireTimeMs) {}

    /**
     * Called when a stream key is encountered.
     * Streams are complex structures; this provides basic notification.
     *
     * @param key the key
     * @param expireTimeMs expiration time in milliseconds since epoch, or -1 if no expiration
     */
    default void onStream(byte[] key, long expireTimeMs) {}

    /**
     * Called when a module value is encountered.
     *
     * @param key the key
     * @param moduleName the module name
     * @param expireTimeMs expiration time in milliseconds since epoch, or -1 if no expiration
     */
    default void onModule(byte[] key, String moduleName, long expireTimeMs) {}

    /**
     * Called when RDB parsing ends successfully.
     *
     * @param checksum the 8-byte CRC64 checksum from the RDB (may be all zeros if disabled)
     */
    default void onRdbEnd(byte[] checksum) {}

    /**
     * Called when an error occurs during parsing.
     *
     * @param error the exception that occurred
     */
    default void onError(Exception error) {}
}
