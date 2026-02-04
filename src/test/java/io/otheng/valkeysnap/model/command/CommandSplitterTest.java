package io.otheng.valkeysnap.model.command;

import io.otheng.valkeysnap.model.keyvalue.*;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

class CommandSplitterTest {

    private final CommandSplitter splitter = new CommandSplitter(ChunkingStrategy.DEFAULT);

    @Test
    void splitString() {
        StringKeyValue kv = new StringKeyValue(
            "mykey".getBytes(StandardCharsets.UTF_8),
            "myvalue".getBytes(StandardCharsets.UTF_8),
            -1,
            0
        );

        List<CommandEvent> events = splitter.split(kv);

        assertThat(events).hasSize(1);
        CommandEvent event = events.get(0);
        assertThat(event.command().name()).isEqualTo("SET");
        assertThat(event.command().argsAsStrings()).containsExactly("mykey", "myvalue");
        assertThat(event.isFirst()).isTrue();
        assertThat(event.isLast()).isTrue();
        assertThat(event.isChunked()).isFalse();
    }

    @Test
    void splitStringWithExpiration() {
        StringKeyValue kv = new StringKeyValue(
            "mykey".getBytes(StandardCharsets.UTF_8),
            "myvalue".getBytes(StandardCharsets.UTF_8),
            1609459200000L, // 2021-01-01 00:00:00 UTC
            0
        );

        List<CommandEvent> events = splitter.split(kv);

        assertThat(events).hasSize(2);

        assertThat(events.get(0).command().name()).isEqualTo("SET");
        assertThat(events.get(0).sequenceNumber()).isEqualTo(1);
        assertThat(events.get(0).totalCommands()).isEqualTo(2);
        assertThat(events.get(0).isFirst()).isTrue();
        assertThat(events.get(0).isLast()).isFalse();

        assertThat(events.get(1).command().name()).isEqualTo("PEXPIREAT");
        assertThat(events.get(1).command().argsAsStrings()).containsExactly("mykey", "1609459200000");
        assertThat(events.get(1).sequenceNumber()).isEqualTo(2);
        assertThat(events.get(1).isLast()).isTrue();
    }

    @Test
    void splitList() {
        List<byte[]> values = List.of(
            "a".getBytes(StandardCharsets.UTF_8),
            "b".getBytes(StandardCharsets.UTF_8),
            "c".getBytes(StandardCharsets.UTF_8)
        );
        ListKeyValue kv = new ListKeyValue(
            "mylist".getBytes(StandardCharsets.UTF_8),
            values,
            -1,
            0
        );

        List<CommandEvent> events = splitter.split(kv);

        assertThat(events).hasSize(1);
        assertThat(events.get(0).command().name()).isEqualTo("RPUSH");
        assertThat(events.get(0).command().argsAsStrings()).containsExactly("mylist", "a", "b", "c");
    }

    @Test
    void splitSet() {
        List<byte[]> members = List.of(
            "x".getBytes(StandardCharsets.UTF_8),
            "y".getBytes(StandardCharsets.UTF_8),
            "z".getBytes(StandardCharsets.UTF_8)
        );
        SetKeyValue kv = new SetKeyValue(
            "myset".getBytes(StandardCharsets.UTF_8),
            members,
            -1,
            0
        );

        List<CommandEvent> events = splitter.split(kv);

        assertThat(events).hasSize(1);
        assertThat(events.get(0).command().name()).isEqualTo("SADD");
        assertThat(events.get(0).command().argsAsStrings()).containsExactly("myset", "x", "y", "z");
    }

    @Test
    void splitSortedSet() {
        List<SortedSetKeyValue.ScoredMember> entries = List.of(
            new SortedSetKeyValue.ScoredMember("alice".getBytes(StandardCharsets.UTF_8), 100.0),
            new SortedSetKeyValue.ScoredMember("bob".getBytes(StandardCharsets.UTF_8), 90.5)
        );
        SortedSetKeyValue kv = new SortedSetKeyValue(
            "scores".getBytes(StandardCharsets.UTF_8),
            entries,
            -1,
            0
        );

        List<CommandEvent> events = splitter.split(kv);

        assertThat(events).hasSize(1);
        assertThat(events.get(0).command().name()).isEqualTo("ZADD");
        assertThat(events.get(0).command().argsAsStrings())
            .containsExactly("scores", "100", "alice", "90.5", "bob");
    }

    @Test
    void splitHash() {
        List<HashKeyValue.HashField> fields = List.of(
            new HashKeyValue.HashField(
                "name".getBytes(StandardCharsets.UTF_8),
                "John".getBytes(StandardCharsets.UTF_8)
            ),
            new HashKeyValue.HashField(
                "age".getBytes(StandardCharsets.UTF_8),
                "30".getBytes(StandardCharsets.UTF_8)
            )
        );
        HashKeyValue kv = new HashKeyValue(
            "user:1".getBytes(StandardCharsets.UTF_8),
            fields,
            -1,
            0
        );

        List<CommandEvent> events = splitter.split(kv);

        assertThat(events).hasSize(1);
        assertThat(events.get(0).command().name()).isEqualTo("HSET");
        assertThat(events.get(0).command().argsAsStrings())
            .containsExactly("user:1", "name", "John", "age", "30");
    }

    @Test
    void splitLargeListIntoChunks() {
        // Create a large list that exceeds max elements
        CommandSplitter smallChunkSplitter = new CommandSplitter(ChunkingStrategy.byElements(3));

        List<byte[]> values = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            values.add(String.valueOf(i).getBytes(StandardCharsets.UTF_8));
        }
        ListKeyValue kv = new ListKeyValue(
            "biglist".getBytes(StandardCharsets.UTF_8),
            values,
            -1,
            0
        );

        List<CommandEvent> events = smallChunkSplitter.split(kv);

        // Should be 4 chunks: [0,1,2], [3,4,5], [6,7,8], [9]
        assertThat(events).hasSize(4);

        assertThat(events.get(0).sequenceNumber()).isEqualTo(1);
        assertThat(events.get(0).totalCommands()).isEqualTo(4);
        assertThat(events.get(0).command().argsAsStrings()).containsExactly("biglist", "0", "1", "2");

        assertThat(events.get(1).sequenceNumber()).isEqualTo(2);
        assertThat(events.get(1).command().argsAsStrings()).containsExactly("biglist", "3", "4", "5");

        assertThat(events.get(2).sequenceNumber()).isEqualTo(3);
        assertThat(events.get(2).command().argsAsStrings()).containsExactly("biglist", "6", "7", "8");

        assertThat(events.get(3).sequenceNumber()).isEqualTo(4);
        assertThat(events.get(3).isLast()).isTrue();
        assertThat(events.get(3).command().argsAsStrings()).containsExactly("biglist", "9");
    }

    @Test
    void splitLargeSetWithExpiration() {
        CommandSplitter smallChunkSplitter = new CommandSplitter(ChunkingStrategy.byElements(2));

        List<byte[]> members = List.of(
            "a".getBytes(StandardCharsets.UTF_8),
            "b".getBytes(StandardCharsets.UTF_8),
            "c".getBytes(StandardCharsets.UTF_8)
        );
        SetKeyValue kv = new SetKeyValue(
            "myset".getBytes(StandardCharsets.UTF_8),
            members,
            5000L,
            0
        );

        List<CommandEvent> events = smallChunkSplitter.split(kv);

        // Should be 2 SADD chunks + 1 PEXPIREAT
        assertThat(events).hasSize(3);

        assertThat(events.get(0).command().name()).isEqualTo("SADD");
        assertThat(events.get(0).totalCommands()).isEqualTo(3);

        assertThat(events.get(1).command().name()).isEqualTo("SADD");

        assertThat(events.get(2).command().name()).isEqualTo("PEXPIREAT");
        assertThat(events.get(2).isLast()).isTrue();
    }

    @Test
    void splitEmptyList() {
        ListKeyValue kv = new ListKeyValue(
            "emptylist".getBytes(StandardCharsets.UTF_8),
            List.of(),
            -1,
            0
        );

        List<CommandEvent> events = splitter.split(kv);

        assertThat(events).isEmpty();
    }

    @Test
    void commandToResp() {
        Command cmd = Command.of("SET", "key", "value");
        byte[] resp = cmd.toResp();

        String expected = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        assertThat(new String(resp, StandardCharsets.UTF_8)).isEqualTo(expected);
    }

    @Test
    void commandWithBinaryData() {
        byte[] binaryValue = new byte[]{0x00, 0x01, 0x02, (byte) 0xFF};
        Command cmd = Command.ofBytes("SET", "key".getBytes(StandardCharsets.UTF_8), binaryValue);

        assertThat(cmd.name()).isEqualTo("SET");
        assertThat(cmd.args()).hasSize(2);
        assertThat(cmd.args().get(1)).containsExactly(0x00, 0x01, 0x02, (byte) 0xFF);
    }

    @Test
    void sortedSetWithSpecialScores() {
        List<SortedSetKeyValue.ScoredMember> entries = List.of(
            new SortedSetKeyValue.ScoredMember("inf".getBytes(StandardCharsets.UTF_8), Double.POSITIVE_INFINITY),
            new SortedSetKeyValue.ScoredMember("ninf".getBytes(StandardCharsets.UTF_8), Double.NEGATIVE_INFINITY),
            new SortedSetKeyValue.ScoredMember("int".getBytes(StandardCharsets.UTF_8), 42.0)
        );
        SortedSetKeyValue kv = new SortedSetKeyValue(
            "special".getBytes(StandardCharsets.UTF_8),
            entries,
            -1,
            0
        );

        List<CommandEvent> events = splitter.split(kv);

        assertThat(events.get(0).command().argsAsStrings())
            .containsExactly("special", "+inf", "inf", "-inf", "ninf", "42", "int");
    }
}
