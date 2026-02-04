package io.otheng.valkeysnap.consumer.events;

/**
 * Event emitted when PSYNC handshake completes successfully.
 *
 * @param replId the master's replication ID
 * @param replOffset the replication offset
 * @param fullSync true if this is a full sync, false for partial sync
 */
public record PsyncStartEvent(
    String replId,
    long replOffset,
    boolean fullSync
) {}
