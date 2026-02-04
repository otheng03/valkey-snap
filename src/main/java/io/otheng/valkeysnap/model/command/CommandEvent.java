package io.otheng.valkeysnap.model.command;

/**
 * Event containing a Redis command generated from a KeyValue.
 *
 * @param command the Redis command
 * @param sourceKey the key this command was generated from (may be null for non-key commands)
 * @param db the database number
 * @param sequenceNumber sequence number for ordering commands from the same key
 * @param totalCommands total number of commands for this key (for chunked operations)
 */
public record CommandEvent(
    Command command,
    byte[] sourceKey,
    int db,
    int sequenceNumber,
    int totalCommands
) {
    public CommandEvent {
        command = command != null ? command : Command.of("PING");
        sourceKey = sourceKey != null ? sourceKey.clone() : null;
    }

    /**
     * Creates a simple command event with sequence 1 of 1.
     */
    public static CommandEvent of(Command command, byte[] sourceKey, int db) {
        return new CommandEvent(command, sourceKey, db, 1, 1);
    }

    /**
     * Creates a command event for a chunked operation.
     */
    public static CommandEvent chunked(Command command, byte[] sourceKey, int db, int seq, int total) {
        return new CommandEvent(command, sourceKey, db, seq, total);
    }

    public byte[] sourceKey() {
        return sourceKey != null ? sourceKey.clone() : null;
    }

    /**
     * Returns true if this is the first command for the key.
     */
    public boolean isFirst() {
        return sequenceNumber == 1;
    }

    /**
     * Returns true if this is the last command for the key.
     */
    public boolean isLast() {
        return sequenceNumber == totalCommands;
    }

    /**
     * Returns true if this key was split into multiple commands.
     */
    public boolean isChunked() {
        return totalCommands > 1;
    }

    @Override
    public String toString() {
        if (totalCommands > 1) {
            return command.toString() + " [" + sequenceNumber + "/" + totalCommands + "]";
        }
        return command.toString();
    }
}
