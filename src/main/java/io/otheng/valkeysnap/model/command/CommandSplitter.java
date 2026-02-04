package io.otheng.valkeysnap.model.command;

import io.otheng.valkeysnap.model.keyvalue.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Converts KeyValueEvent objects into Command objects.
 *
 * <p>Handles chunking of large collections to avoid oversized commands.
 * TTL is emitted as a separate PEXPIREAT command when present.
 *
 * <p>Command mapping:
 * <ul>
 *   <li>STRING → SET key value [+ PEXPIREAT]</li>
 *   <li>LIST → RPUSH key value... [+ PEXPIREAT]</li>
 *   <li>SET → SADD key member... [+ PEXPIREAT]</li>
 *   <li>ZSET → ZADD key score member... [+ PEXPIREAT]</li>
 *   <li>HASH → HSET key field value... [+ PEXPIREAT]</li>
 * </ul>
 */
public class CommandSplitter {

    private final ChunkingStrategy strategy;

    public CommandSplitter() {
        this(ChunkingStrategy.DEFAULT);
    }

    public CommandSplitter(ChunkingStrategy strategy) {
        this.strategy = strategy;
    }

    /**
     * Splits a KeyValueEvent into one or more CommandEvents.
     *
     * @param event the key-value event to convert
     * @return list of command events
     */
    public List<CommandEvent> split(KeyValueEvent event) {
        List<CommandEvent> commands = new ArrayList<>();
        split(event, commands::add);
        return commands;
    }

    /**
     * Splits a KeyValueEvent and emits CommandEvents to the consumer.
     *
     * @param event the key-value event to convert
     * @param consumer the command event consumer
     */
    public void split(KeyValueEvent event, Consumer<CommandEvent> consumer) {
        if (event instanceof StringKeyValue s) {
            splitString(s, consumer);
        } else if (event instanceof ListKeyValue l) {
            splitList(l, consumer);
        } else if (event instanceof SetKeyValue s) {
            splitSet(s, consumer);
        } else if (event instanceof SortedSetKeyValue z) {
            splitSortedSet(z, consumer);
        } else if (event instanceof HashKeyValue h) {
            splitHash(h, consumer);
        } else if (event instanceof StreamKeyValue st) {
            splitStream(st, consumer);
        } else if (event instanceof ModuleKeyValue m) {
            splitModule(m, consumer);
        }
    }

    private void splitString(StringKeyValue event, Consumer<CommandEvent> consumer) {
        byte[] key = event.key();
        Command setCmd = Command.ofBytes("SET", key, event.value());

        if (event.hasExpiration()) {
            consumer.accept(CommandEvent.chunked(setCmd, key, event.db(), 1, 2));
            emitExpire(key, event.expireTimeMs(), event.db(), 2, 2, consumer);
        } else {
            consumer.accept(CommandEvent.of(setCmd, key, event.db()));
        }
    }

    private void splitList(ListKeyValue event, Consumer<CommandEvent> consumer) {
        byte[] key = event.key();
        List<byte[]> values = event.values();

        if (values.isEmpty()) {
            return;
        }

        List<List<byte[]>> chunks = chunkByteArrays(values);
        int total = chunks.size() + (event.hasExpiration() ? 1 : 0);
        int seq = 1;

        for (List<byte[]> chunk : chunks) {
            List<byte[]> args = new ArrayList<>(chunk.size() + 1);
            args.add(key);
            args.addAll(chunk);
            Command cmd = new Command("RPUSH", args);
            consumer.accept(CommandEvent.chunked(cmd, key, event.db(), seq++, total));
        }

        if (event.hasExpiration()) {
            emitExpire(key, event.expireTimeMs(), event.db(), seq, total, consumer);
        }
    }

    private void splitSet(SetKeyValue event, Consumer<CommandEvent> consumer) {
        byte[] key = event.key();
        List<byte[]> members = event.members();

        if (members.isEmpty()) {
            return;
        }

        List<List<byte[]>> chunks = chunkByteArrays(members);
        int total = chunks.size() + (event.hasExpiration() ? 1 : 0);
        int seq = 1;

        for (List<byte[]> chunk : chunks) {
            List<byte[]> args = new ArrayList<>(chunk.size() + 1);
            args.add(key);
            args.addAll(chunk);
            Command cmd = new Command("SADD", args);
            consumer.accept(CommandEvent.chunked(cmd, key, event.db(), seq++, total));
        }

        if (event.hasExpiration()) {
            emitExpire(key, event.expireTimeMs(), event.db(), seq, total, consumer);
        }
    }

    private void splitSortedSet(SortedSetKeyValue event, Consumer<CommandEvent> consumer) {
        byte[] key = event.key();
        List<SortedSetKeyValue.ScoredMember> entries = event.entries();

        if (entries.isEmpty()) {
            return;
        }

        // ZADD key score member [score member ...]
        // Each entry is 2 elements (score + member)
        int effectiveMaxElements = strategy.maxElements() / 2;
        if (effectiveMaxElements < 1) effectiveMaxElements = 1;

        List<List<SortedSetKeyValue.ScoredMember>> chunks = new ArrayList<>();
        for (int i = 0; i < entries.size(); i += effectiveMaxElements) {
            chunks.add(entries.subList(i, Math.min(i + effectiveMaxElements, entries.size())));
        }

        int total = chunks.size() + (event.hasExpiration() ? 1 : 0);
        int seq = 1;

        for (List<SortedSetKeyValue.ScoredMember> chunk : chunks) {
            List<byte[]> args = new ArrayList<>(chunk.size() * 2 + 1);
            args.add(key);
            for (SortedSetKeyValue.ScoredMember entry : chunk) {
                args.add(formatScore(entry.score()));
                args.add(entry.member());
            }
            Command cmd = new Command("ZADD", args);
            consumer.accept(CommandEvent.chunked(cmd, key, event.db(), seq++, total));
        }

        if (event.hasExpiration()) {
            emitExpire(key, event.expireTimeMs(), event.db(), seq, total, consumer);
        }
    }

    private void splitHash(HashKeyValue event, Consumer<CommandEvent> consumer) {
        byte[] key = event.key();
        List<HashKeyValue.HashField> fields = event.fields();

        if (fields.isEmpty()) {
            return;
        }

        // HSET key field value [field value ...]
        // Each field is 2 elements (field + value)
        int effectiveMaxElements = strategy.maxElements() / 2;
        if (effectiveMaxElements < 1) effectiveMaxElements = 1;

        List<List<HashKeyValue.HashField>> chunks = new ArrayList<>();
        for (int i = 0; i < fields.size(); i += effectiveMaxElements) {
            chunks.add(fields.subList(i, Math.min(i + effectiveMaxElements, fields.size())));
        }

        int total = chunks.size() + (event.hasExpiration() ? 1 : 0);
        int seq = 1;

        for (List<HashKeyValue.HashField> chunk : chunks) {
            List<byte[]> args = new ArrayList<>(chunk.size() * 2 + 1);
            args.add(key);
            for (HashKeyValue.HashField field : chunk) {
                args.add(field.field());
                args.add(field.value());
            }
            Command cmd = new Command("HSET", args);
            consumer.accept(CommandEvent.chunked(cmd, key, event.db(), seq++, total));
        }

        if (event.hasExpiration()) {
            emitExpire(key, event.expireTimeMs(), event.db(), seq, total, consumer);
        }
    }

    private void splitStream(StreamKeyValue event, Consumer<CommandEvent> consumer) {
        // Streams are complex - for now we just notify
        // A full implementation would need to reconstruct XADD commands
    }

    private void splitModule(ModuleKeyValue event, Consumer<CommandEvent> consumer) {
        // Module data cannot be reconstructed without the module
    }

    private void emitExpire(byte[] key, long expireTimeMs, int db, int seq, int total, Consumer<CommandEvent> consumer) {
        Command expireCmd = Command.of("PEXPIREAT",
            new String(key, StandardCharsets.UTF_8),
            String.valueOf(expireTimeMs));
        consumer.accept(CommandEvent.chunked(expireCmd, key, db, seq, total));
    }

    private List<List<byte[]>> chunkByteArrays(List<byte[]> items) {
        List<List<byte[]>> chunks = new ArrayList<>();
        List<byte[]> currentChunk = new ArrayList<>();
        long currentBytes = 0;

        for (byte[] item : items) {
            if (!currentChunk.isEmpty() &&
                (currentChunk.size() >= strategy.maxElements() ||
                 currentBytes + item.length > strategy.maxBytes())) {
                chunks.add(currentChunk);
                currentChunk = new ArrayList<>();
                currentBytes = 0;
            }
            currentChunk.add(item);
            currentBytes += item.length;
        }

        if (!currentChunk.isEmpty()) {
            chunks.add(currentChunk);
        }

        return chunks;
    }

    private byte[] formatScore(double score) {
        if (score == Double.POSITIVE_INFINITY) {
            return "+inf".getBytes(StandardCharsets.UTF_8);
        } else if (score == Double.NEGATIVE_INFINITY) {
            return "-inf".getBytes(StandardCharsets.UTF_8);
        } else if (Double.isNaN(score)) {
            return "nan".getBytes(StandardCharsets.UTF_8);
        } else if (score == Math.floor(score) && !Double.isInfinite(score)) {
            return String.valueOf((long) score).getBytes(StandardCharsets.UTF_8);
        } else {
            return String.valueOf(score).getBytes(StandardCharsets.UTF_8);
        }
    }
}
