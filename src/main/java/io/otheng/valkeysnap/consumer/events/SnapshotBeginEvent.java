package io.otheng.valkeysnap.consumer.events;

/**
 * Event emitted when RDB snapshot parsing begins.
 *
 * @param rdbVersion the RDB format version
 */
public record SnapshotBeginEvent(
    int rdbVersion
) {}
