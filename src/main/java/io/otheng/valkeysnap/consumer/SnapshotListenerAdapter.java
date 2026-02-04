package io.otheng.valkeysnap.consumer;

import io.otheng.valkeysnap.consumer.events.*;
import io.otheng.valkeysnap.model.command.CommandEvent;
import io.otheng.valkeysnap.model.keyvalue.KeyValueEvent;
import io.otheng.valkeysnap.model.raw.RawBytesEvent;

/**
 * Adapter class providing default no-op implementations for {@link SnapshotListener}.
 *
 * <p>Extend this class and override only the methods you're interested in.
 *
 * <p>Example usage:
 * <pre>{@code
 * SnapshotListener listener = new SnapshotListenerAdapter() {
 *     @Override
 *     public void onKeyValue(KeyValueEvent event) {
 *         System.out.println("Got key: " + event.keyAsString());
 *     }
 *
 *     @Override
 *     public void onError(Throwable error) {
 *         error.printStackTrace();
 *     }
 * };
 * }</pre>
 */
public class SnapshotListenerAdapter implements SnapshotListener {

    @Override
    public void onPsyncStart(PsyncStartEvent event) {
        // No-op by default
    }

    @Override
    public void onSnapshotBegin(SnapshotBeginEvent event) {
        // No-op by default
    }

    @Override
    public void onDbSelect(DbSelectEvent event) {
        // No-op by default
    }

    @Override
    public void onKeyValue(KeyValueEvent event) {
        // No-op by default
    }

    @Override
    public void onCommand(CommandEvent event) {
        // No-op by default
    }

    @Override
    public void onRawBytes(RawBytesEvent event) {
        // No-op by default
    }

    @Override
    public void onSnapshotEnd(SnapshotEndEvent event) {
        // No-op by default
    }

    @Override
    public void onPsyncEnd(PsyncEndEvent event) {
        // No-op by default
    }

    @Override
    public void onError(Throwable error) {
        // No-op by default - subclasses should override to handle errors
    }
}
