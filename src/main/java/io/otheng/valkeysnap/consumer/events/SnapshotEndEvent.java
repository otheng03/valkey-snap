package io.otheng.valkeysnap.consumer.events;

/**
 * Event emitted when RDB snapshot parsing completes.
 *
 * @param checksum the RDB checksum (may be zeros if checksum was disabled)
 * @param totalKeys total number of keys parsed
 */
public record SnapshotEndEvent(
    byte[] checksum,
    long totalKeys
) {
    public SnapshotEndEvent {
        checksum = checksum != null ? checksum.clone() : new byte[8];
    }

    @Override
    public byte[] checksum() {
        return checksum.clone();
    }
}
