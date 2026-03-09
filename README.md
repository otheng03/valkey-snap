# valkey-snap

PSYNC-based snapshot replication utility for Valkey/Redis in Java 17.

Connects to a Valkey or Redis server as a replica and streams the full dataset snapshot via the PSYNC protocol. Delivers data to your application as structured key-value events, replayable Redis commands, or raw RDB bytes.

## Requirements

- Java 17+
- Gradle 9.0+

## Build

```bash
./gradlew build
```

## Test

```bash
./gradlew test
```

## Usage

### KeyValue Mode (default)

Receive a `KeyValueEvent` for each key in the snapshot:

```java
ValkeySnap.builder()
    .host("localhost")
    .port(6379)
    .emitKeyValueEvents(true)
    .listener(new SnapshotListenerAdapter() {
        @Override
        public void onKeyValue(KeyValueEvent event) {
            System.out.println(event.keyAsString() + " -> " + event.typeName());
        }
    })
    .build()
    .start();
```

`KeyValueEvent` is a sealed interface — use pattern matching to handle each type:

```java
switch (event) {
    case StringKeyValue kv    -> System.out.println(kv.value());
    case HashKeyValue kv      -> kv.fields().forEach((k, v) -> ...);
    case ListKeyValue kv      -> kv.elements().forEach(...);
    case SetKeyValue kv       -> kv.members().forEach(...);
    case SortedSetKeyValue kv -> kv.entries().forEach(...);
    case StreamKeyValue kv    -> { /* notification only */ }
    case ModuleKeyValue kv    -> { /* notification only */ }
}
```

### Command Mode

Receive replayable Redis commands as `CommandEvent`:

```java
ValkeySnap.builder()
    .host("localhost")
    .emitCommandEvents(true)
    .commandChunkMaxElements(1000)
    .listener(new SnapshotListenerAdapter() {
        @Override
        public void onCommand(CommandEvent event) {
            byte[] resp = event.command().toResp();
            outputStream.write(resp);
        }
    })
    .build()
    .start();
```

Large collections are automatically chunked to avoid oversized commands.

| Redis Type | Commands Generated          |
|------------|-----------------------------|
| STRING     | `SET key value`             |
| LIST       | `RPUSH key value...`        |
| SET        | `SADD key member...`        |
| ZSET       | `ZADD key score member...`  |
| HASH       | `HSET key field value...`   |
| With TTL   | + `PEXPIREAT key timestamp` |

### Raw Bytes Mode

Receive the raw RDB stream as `RawBytesEvent` for pass-through forwarding:

```java
ValkeySnap.builder()
    .host("localhost")
    .emitRawBytes(true)
    .emitKeyValueEvents(false)
    .listener(new SnapshotListenerAdapter() {
        @Override
        public void onRawBytes(RawBytesEvent event) {
            outputStream.write(event.data());
        }
    })
    .build()
    .start();
```

## Lifecycle Events

Implement `SnapshotListener` (or extend `SnapshotListenerAdapter`) to handle lifecycle callbacks:

| Method            | When it fires                             |
|-------------------|-------------------------------------------|
| `onPsyncStart`    | PSYNC handshake completed                 |
| `onSnapshotBegin` | RDB parsing started                       |
| `onDbSelect`      | Database selection encountered in RDB     |
| `onKeyValue`      | Key parsed from RDB (KeyValue mode)       |
| `onCommand`       | Command generated from key (Command mode) |
| `onRawBytes`      | Raw RDB bytes available (Raw mode)        |
| `onSnapshotEnd`   | RDB parsing completed                     |
| `onPsyncEnd`      | Replication session ended                 |

## Architecture

```
+--------------------------------------------------------------+
|                Consumer / User Interface                     |
+------------------------------▲-------------------------------+
                               |
+------------------------------|-------------------------------+
|              Redis Command Model Layer                       |
+------------------------------▲-------------------------------+
                               |
+------------------------------|-------------------------------+
|           Application Protocol Layer (RESP/RDB)              |
+------------------------------▲-------------------------------+
                               |
+------------------------------|-------------------------------+
|                    Network Layer                             |
+--------------------------------------------------------------+
```

## RDB Types Supported

TBD

| Type   | Encodings                        | Support |
|--------|----------------------------------|---------|
| STRING | plain, int, LZF-compressed       |    |
| LIST   | linked list, quicklist, listpack |    |
| SET    | hashtable, intset, listpack      |    |
| ZSET   | skiplist, ziplist, listpack      |    |
| HASH   | hashtable, ziplist, listpack     |    |
| STREAM | all                              |    |
| MODULE | all                              |    |

## Dependencies

Minimize external dependencies to keep the library small and make easy to maintain.

| Scope   | Artifact                 | Version |
|---------|--------------------------|---------|
| Runtime | `org.slf4j:slf4j-api`    | 2.0.9   |
| Test    | JUnit 5                  | 5.10.0  |
| Test    | AssertJ                  | 3.27.7  |
| Test    | Mockito                  | 5.7.0   |
| Test    | Logback                  | 1.5.13  |
| Test    | Testcontainers           | 1.19.3  |
| Test    | Jedis                    | 5.1.0   |