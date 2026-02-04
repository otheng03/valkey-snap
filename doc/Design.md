# Design Document

Valkey PSYNC-based Snapshot Replication Utility

##  Design Goals

- Decode snapshot into structured events (key/value)
- Allow splitting snapshot data into individual Redis commands
- Optionally expose raw bytes (no conversion) for direct forwarding to another Redis/Valkey

# Architecture
```
+--------------------------------------------------------------+
|                Consumer / User Interface                     |
|   - Structured Events (KeyValue / Command)                   |
|   - Raw Bytes (pass-through mode)                            |
+------------------------------▲-------------------------------+
                               |
+------------------------------|-------------------------------+
|              Redis Command Model Layer                       |
|   - KeyValue -> Command Splitter (e.g., SET/HSET/SADD/...)   |
|   - Command events                                           |
+------------------------------▲-------------------------------+
                               |
+------------------------------|-------------------------------+
|           Application Protocol Layer (RESP/RDB)              |
|   - RESP parsing                                             |
|   - RDB stream extraction                                    |
|   - (Optional) raw byte stream tap                           |
+------------------------------▲-------------------------------+
                               |
+------------------------------|-------------------------------+
|                    Network Layer                             |
|   - Java 17 sockets                                          |
|   - PSYNC/REPLCONF                                           |
|   - Rate limit                                               |
+--------------------------------------------------------------+
```

## Redis Command Model Layer
### Responsibility

Convert snapshot data into models that can be consumed at different levels:

- Key-level representation (KeyValueEvent)
- Command-level representation (individual Redis commands)
- (Optional) Raw bytes pass-through (no conversion)

### KeyValue → Command Splitter

KeyValue events must be splittable into one or more canonical Redis commands.

Example mappings

- String: SET key value (+ PEXPIRE if TTL exists)
- Hash: multiple HSET key field value chunks or batched forms
- Set: SADD key member... (chunking if huge)
- ZSet: ZADD key score member...
- List: RPUSH/LPUSH key ... (depending on reconstruction strategy)

Notes

- Chunking is required to avoid oversized commands (configurable max elements / max bytes)
- TTL should be emitted as a separate command (e.g., PEXPIRE) or attached metadata

Outputs
- CommandEvent (structured command model: command name + args)
- (Optional) RespCommandBytesEvent (RESP-encoded bytes ready to send)

## Consumer / User Interface Layer

Listener Interface

```Java
public interface SnapshotListener {
    void onPsyncStart(PsyncStartEvent event);
    void onSnapshotBegin(SnapshotBeginEvent event);

    // 1) Structured key-level events
    void onKeyValue(KeyValueEvent event);

    // 2) Structured command-level events (from KeyValue -> splitter)
    void onCommand(CommandEvent event);

    // 3) Raw RESP bytes for pass-through (no transformation)
    void onRawBytes(RawBytesEvent event);

    void onSnapshotEnd(SnapshotEndEvent event);
    void onPsyncEnd(PsyncEndEvent event);
    void onError(Throwable t);
}

```

### Consumption Modes

Mode A: KeyValue Mode
- Users receive KeyValueEvent
- Useful when users want to transform/filter/inspect data

Mode B: Command Mode
- Utility splits KeyValue into individual commands and emits CommandEvent
- Useful when users want replayable commands or need a command-based pipeline

Mode C: Pass-through Raw Bytes Mode
- Utility emits RawBytesEvent containing raw bytes read from Valkey replication stream
- Intended for direct forwarding to another Redis/Valkey without decoding/encoding overhead

> Important: In Pass-through mode, the tool does not guarantee semantic boundaries like “per key” or “per command”; it delivers byte chunks that are safe for streaming forward.

## Data Flow

- Network reads bytes (rate-limited)
- Protocol parses RESP and extracts snapshot stream
- Depending on user configuration:
  - KeyValue events emitted
  - AND/OR KeyValue → Command split → Command events emitted
  - AND/OR raw bytes emitted (tap)

##  Configuration

- rateLimitBytesPerSecond
- emitKeyValueEvents: boolean
- emitCommandEvents: boolean
- emitRawBytes: boolean
- commandChunkMaxBytes / maxArgs (chunking policy)
