package io.otheng.valkeysnap.consumer.events;

/**
 * Event emitted when the PSYNC replication session ends.
 *
 * @param reason the reason for ending (e.g., "completed", "disconnected", "error")
 */
public record PsyncEndEvent(
    String reason
) {}
