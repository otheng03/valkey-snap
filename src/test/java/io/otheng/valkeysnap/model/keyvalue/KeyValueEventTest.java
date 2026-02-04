package io.otheng.valkeysnap.model.keyvalue;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

class KeyValueEventTest {

    @Test
    void stringKeyValue() {
        StringKeyValue kv = new StringKeyValue(
            "mykey".getBytes(StandardCharsets.UTF_8),
            "myvalue".getBytes(StandardCharsets.UTF_8),
            1000L,
            0
        );

        assertThat(kv.keyAsString()).isEqualTo("mykey");
        assertThat(kv.valueAsString()).isEqualTo("myvalue");
        assertThat(kv.expireTimeMs()).isEqualTo(1000L);
        assertThat(kv.db()).isEqualTo(0);
        assertThat(kv.hasExpiration()).isTrue();
        assertThat(kv.typeName()).isEqualTo("STRING");
    }

    @Test
    void stringKeyValueWithoutExpiration() {
        StringKeyValue kv = new StringKeyValue(
            "key".getBytes(StandardCharsets.UTF_8),
            "value".getBytes(StandardCharsets.UTF_8),
            -1,
            0
        );

        assertThat(kv.hasExpiration()).isFalse();
    }

    @Test
    void listKeyValue() {
        List<byte[]> values = List.of(
            "a".getBytes(StandardCharsets.UTF_8),
            "b".getBytes(StandardCharsets.UTF_8),
            "c".getBytes(StandardCharsets.UTF_8)
        );
        ListKeyValue kv = new ListKeyValue(
            "mylist".getBytes(StandardCharsets.UTF_8),
            values,
            -1,
            1
        );

        assertThat(kv.keyAsString()).isEqualTo("mylist");
        assertThat(kv.size()).isEqualTo(3);
        assertThat(kv.valuesAsStrings()).containsExactly("a", "b", "c");
        assertThat(kv.db()).isEqualTo(1);
        assertThat(kv.typeName()).isEqualTo("LIST");
    }

    @Test
    void setKeyValue() {
        List<byte[]> members = List.of(
            "x".getBytes(StandardCharsets.UTF_8),
            "y".getBytes(StandardCharsets.UTF_8)
        );
        SetKeyValue kv = new SetKeyValue(
            "myset".getBytes(StandardCharsets.UTF_8),
            members,
            -1,
            0
        );

        assertThat(kv.keyAsString()).isEqualTo("myset");
        assertThat(kv.size()).isEqualTo(2);
        assertThat(kv.membersAsStrings()).containsExactlyInAnyOrder("x", "y");
        assertThat(kv.typeName()).isEqualTo("SET");
    }

    @Test
    void sortedSetKeyValue() {
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

        assertThat(kv.keyAsString()).isEqualTo("scores");
        assertThat(kv.size()).isEqualTo(2);
        assertThat(kv.entriesAsStringMap())
            .containsEntry("alice", 100.0)
            .containsEntry("bob", 90.5);
        assertThat(kv.typeName()).isEqualTo("ZSET");
    }

    @Test
    void hashKeyValue() {
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
            5000L,
            0
        );

        assertThat(kv.keyAsString()).isEqualTo("user:1");
        assertThat(kv.size()).isEqualTo(2);
        assertThat(kv.fieldsAsStringMap())
            .containsEntry("name", "John")
            .containsEntry("age", "30");
        assertThat(kv.hasExpiration()).isTrue();
        assertThat(kv.typeName()).isEqualTo("HASH");
    }

    @Test
    void keyValueImmutability() {
        byte[] originalKey = "key".getBytes(StandardCharsets.UTF_8);
        byte[] originalValue = "value".getBytes(StandardCharsets.UTF_8);

        StringKeyValue kv = new StringKeyValue(originalKey, originalValue, -1, 0);

        // Modify originals
        originalKey[0] = 'X';
        originalValue[0] = 'X';

        // kv should not be affected
        assertThat(kv.keyAsString()).isEqualTo("key");
        assertThat(kv.valueAsString()).isEqualTo("value");

        // Returned arrays should also be copies
        byte[] returnedKey = kv.key();
        returnedKey[0] = 'Y';
        assertThat(kv.keyAsString()).isEqualTo("key");
    }

    @Test
    void streamKeyValue() {
        StreamKeyValue kv = new StreamKeyValue(
            "mystream".getBytes(StandardCharsets.UTF_8),
            -1,
            0
        );

        assertThat(kv.keyAsString()).isEqualTo("mystream");
        assertThat(kv.typeName()).isEqualTo("STREAM");
    }

    @Test
    void moduleKeyValue() {
        ModuleKeyValue kv = new ModuleKeyValue(
            "modkey".getBytes(StandardCharsets.UTF_8),
            "ReJSON",
            -1,
            0
        );

        assertThat(kv.keyAsString()).isEqualTo("modkey");
        assertThat(kv.moduleName()).isEqualTo("ReJSON");
        assertThat(kv.typeName()).isEqualTo("MODULE");
    }

    @Test
    void sealedInterfacePatternMatching() {
        KeyValueEvent event = new StringKeyValue(
            "test".getBytes(StandardCharsets.UTF_8),
            "value".getBytes(StandardCharsets.UTF_8),
            -1,
            0
        );

        String result;
        if (event instanceof StringKeyValue s) {
            result = "string: " + s.valueAsString();
        } else if (event instanceof ListKeyValue l) {
            result = "list: " + l.size() + " items";
        } else if (event instanceof SetKeyValue s) {
            result = "set: " + s.size() + " members";
        } else if (event instanceof SortedSetKeyValue z) {
            result = "zset: " + z.size() + " entries";
        } else if (event instanceof HashKeyValue h) {
            result = "hash: " + h.size() + " fields";
        } else if (event instanceof StreamKeyValue st) {
            result = "stream";
        } else if (event instanceof ModuleKeyValue m) {
            result = "module: " + m.moduleName();
        } else {
            result = "unknown";
        }

        assertThat(result).isEqualTo("string: value");
    }
}
