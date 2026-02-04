package io.otheng.valkeysnap.consumer;

import io.otheng.valkeysnap.consumer.events.*;
import io.otheng.valkeysnap.model.command.CommandEvent;
import io.otheng.valkeysnap.model.keyvalue.KeyValueEvent;
import io.otheng.valkeysnap.model.raw.RawBytesEvent;

/**
 * Listener interface for receiving snapshot replication events.
 *
 * <p>Users implement this interface to process events from the replication stream.
 * Events are delivered in the following order:
 * <ol>
 *   <li>{@link #onPsyncStart(PsyncStartEvent)} - PSYNC handshake completed</li>
 *   <li>{@link #onSnapshotBegin(SnapshotBeginEvent)} - RDB parsing started</li>
 *   <li>{@link #onDbSelect(DbSelectEvent)} - Database selected (may occur multiple times)</li>
 *   <li>{@link #onKeyValue(KeyValueEvent)} / {@link #onCommand(CommandEvent)} / {@link #onRawBytes(RawBytesEvent)} - Data events</li>
 *   <li>{@link #onSnapshotEnd(SnapshotEndEvent)} - RDB parsing completed</li>
 *   <li>{@link #onPsyncEnd(PsyncEndEvent)} - Replication session ended</li>
 * </ol>
 *
 * <p>If an error occurs, {@link #onError(Throwable)} is called.
 */
public interface SnapshotListener {

    /**
     * Called when PSYNC handshake completes successfully.
     *
     * @param event the psync start event containing replication ID and offset
     */
    void onPsyncStart(PsyncStartEvent event);

    /**
     * Called when RDB snapshot parsing begins.
     *
     * @param event the snapshot begin event containing RDB version
     */
    void onSnapshotBegin(SnapshotBeginEvent event);

    /**
     * Called when a database is selected during RDB parsing.
     *
     * @param event the database select event
     */
    void onDbSelect(DbSelectEvent event);

    /**
     * Called for each key-value pair parsed from the RDB.
     * Only called if KeyValue mode is enabled.
     *
     * @param event the key-value event (StringKeyValue, ListKeyValue, etc.)
     */
    void onKeyValue(KeyValueEvent event);

    /**
     * Called for each command generated from key-value pairs.
     * Only called if Command mode is enabled.
     *
     * @param event the command event (SET, HSET, SADD, etc.)
     */
    void onCommand(CommandEvent event);

    /**
     * Called with raw bytes read from the replication stream.
     * Only called if Raw Bytes mode is enabled.
     *
     * @param event the raw bytes event
     */
    void onRawBytes(RawBytesEvent event);

    /**
     * Called when RDB snapshot parsing completes successfully.
     *
     * @param event the snapshot end event containing checksum
     */
    void onSnapshotEnd(SnapshotEndEvent event);

    /**
     * Called when the PSYNC replication session ends.
     *
     * @param event the psync end event containing reason
     */
    void onPsyncEnd(PsyncEndEvent event);

    /**
     * Called when an error occurs during replication.
     *
     * @param error the exception that occurred
     */
    void onError(Throwable error);
}
