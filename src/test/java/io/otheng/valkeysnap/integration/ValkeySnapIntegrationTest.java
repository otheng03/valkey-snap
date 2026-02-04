package io.otheng.valkeysnap.integration;

import io.otheng.valkeysnap.consumer.SnapshotListenerAdapter;
import io.otheng.valkeysnap.consumer.events.*;
import io.otheng.valkeysnap.core.ValkeySnap;
import io.otheng.valkeysnap.model.command.CommandEvent;
import io.otheng.valkeysnap.model.keyvalue.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.Jedis;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration test that runs against a real Valkey 9 instance.
 * Tests all data types: STRING, LIST, SET, ZSET, HASH.
 */
@Testcontainers
class ValkeySnapIntegrationTest {

    @Container
    private static final GenericContainer<?> valkey = new GenericContainer<>(
        DockerImageName.parse("valkey/valkey:9")
    )
        .withExposedPorts(6379)
        .withCommand("valkey-server", "--appendonly", "no");

    private Jedis jedis;

    @BeforeEach
    void setUp() {
        jedis = new Jedis(valkey.getHost(), valkey.getMappedPort(6379));
        jedis.flushAll();
    }

    @AfterEach
    void tearDown() {
        if (jedis != null) {
            jedis.close();
        }
    }

    @Test
    void replicateAllDataTypes() throws Exception {
        // Setup test data for all types
        setupTestData();

        // Track received events
        Map<String, KeyValueEvent> receivedKeys = new ConcurrentHashMap<>();
        List<CommandEvent> receivedCommands = Collections.synchronizedList(new ArrayList<>());
        AtomicReference<PsyncStartEvent> psyncStart = new AtomicReference<>();
        AtomicReference<SnapshotBeginEvent> snapshotBegin = new AtomicReference<>();
        AtomicReference<SnapshotEndEvent> snapshotEnd = new AtomicReference<>();
        AtomicBoolean completed = new AtomicBoolean(false);
        AtomicReference<Throwable> error = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        ValkeySnap snap = ValkeySnap.builder()
            .host(valkey.getHost())
            .port(valkey.getMappedPort(6379))
            .emitKeyValueEvents(true)
            .emitCommandEvents(true)
            .connectionTimeout(Duration.ofSeconds(10))
            .readTimeout(Duration.ofSeconds(30))
            .listener(new SnapshotListenerAdapter() {
                @Override
                public void onPsyncStart(PsyncStartEvent event) {
                    psyncStart.set(event);
                    System.out.println("PSYNC started: replId=" + event.replId() +
                        ", offset=" + event.replOffset() + ", fullSync=" + event.fullSync());
                }

                @Override
                public void onSnapshotBegin(SnapshotBeginEvent event) {
                    snapshotBegin.set(event);
                    System.out.println("Snapshot begin: RDB version=" + event.rdbVersion());
                }

                @Override
                public void onKeyValue(KeyValueEvent event) {
                    receivedKeys.put(event.keyAsString(), event);
                    System.out.println("Received key: " + event.keyAsString() + " (" + event.typeName() + ")");
                }

                @Override
                public void onCommand(CommandEvent event) {
                    receivedCommands.add(event);
                }

                @Override
                public void onSnapshotEnd(SnapshotEndEvent event) {
                    snapshotEnd.set(event);
                    System.out.println("Snapshot end: " + event.totalKeys() + " keys");
                }

                @Override
                public void onPsyncEnd(PsyncEndEvent event) {
                    completed.set(true);
                    System.out.println("PSYNC ended: " + event.reason());
                    latch.countDown();
                }

                @Override
                public void onError(Throwable t) {
                    error.set(t);
                    t.printStackTrace();
                    latch.countDown();
                }
            })
            .build();

        // Run replication
        Thread replicationThread = snap.startAsync();
        boolean finished = latch.await(30, TimeUnit.SECONDS);

        // Cleanup
        snap.close();
        replicationThread.join(5000);

        // Assertions
        assertThat(finished).as("Replication should complete within timeout").isTrue();
        assertThat(error.get()).as("No errors should occur").isNull();
        assertThat(completed.get()).as("Replication should complete").isTrue();

        // Verify PSYNC handshake
        assertThat(psyncStart.get()).isNotNull();
        assertThat(psyncStart.get().fullSync()).isTrue();
        assertThat(psyncStart.get().replId()).isNotEmpty();

        // Verify snapshot events
        assertThat(snapshotBegin.get()).isNotNull();
        assertThat(snapshotEnd.get()).isNotNull();
        assertThat(snapshotEnd.get().totalKeys()).isEqualTo(5);

        // Verify all keys received
        assertThat(receivedKeys).hasSize(5);

        // Verify STRING
        assertThat(receivedKeys.get("string:key")).isInstanceOf(StringKeyValue.class);
        StringKeyValue stringKv = (StringKeyValue) receivedKeys.get("string:key");
        assertThat(stringKv.valueAsString()).isEqualTo("hello world");

        // Verify LIST
        assertThat(receivedKeys.get("list:key")).isInstanceOf(ListKeyValue.class);
        ListKeyValue listKv = (ListKeyValue) receivedKeys.get("list:key");
        assertThat(listKv.valuesAsStrings()).containsExactly("a", "b", "c", "d", "e");

        // Verify SET
        assertThat(receivedKeys.get("set:key")).isInstanceOf(SetKeyValue.class);
        SetKeyValue setKv = (SetKeyValue) receivedKeys.get("set:key");
        assertThat(setKv.membersAsStrings()).containsExactlyInAnyOrder("x", "y", "z");

        // Verify ZSET
        assertThat(receivedKeys.get("zset:key")).isInstanceOf(SortedSetKeyValue.class);
        SortedSetKeyValue zsetKv = (SortedSetKeyValue) receivedKeys.get("zset:key");
        Map<String, Double> scores = zsetKv.entriesAsStringMap();
        assertThat(scores).containsEntry("alice", 100.0);
        assertThat(scores).containsEntry("bob", 90.0);
        assertThat(scores).containsEntry("charlie", 80.0);

        // Verify HASH
        assertThat(receivedKeys.get("hash:key")).isInstanceOf(HashKeyValue.class);
        HashKeyValue hashKv = (HashKeyValue) receivedKeys.get("hash:key");
        Map<String, String> fields = hashKv.fieldsAsStringMap();
        assertThat(fields).containsEntry("name", "John Doe");
        assertThat(fields).containsEntry("age", "30");
        assertThat(fields).containsEntry("city", "New York");

        // Verify commands were generated
        assertThat(receivedCommands).isNotEmpty();

        // Count commands by type
        Map<String, Long> commandCounts = new HashMap<>();
        for (CommandEvent cmd : receivedCommands) {
            commandCounts.merge(cmd.command().name(), 1L, Long::sum);
        }
        System.out.println("Command counts: " + commandCounts);

        assertThat(commandCounts).containsKey("SET");
        assertThat(commandCounts).containsKey("RPUSH");
        assertThat(commandCounts).containsKey("SADD");
        assertThat(commandCounts).containsKey("ZADD");
        assertThat(commandCounts).containsKey("HSET");
    }

    @Test
    void replicateWithExpiration() throws Exception {
        // Set key with TTL
        jedis.setex("expiring:key", 3600, "value with ttl");

        Map<String, KeyValueEvent> receivedKeys = new ConcurrentHashMap<>();
        List<CommandEvent> receivedCommands = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(1);

        ValkeySnap snap = ValkeySnap.builder()
            .host(valkey.getHost())
            .port(valkey.getMappedPort(6379))
            .emitKeyValueEvents(true)
            .emitCommandEvents(true)
            .listener(new SnapshotListenerAdapter() {
                @Override
                public void onKeyValue(KeyValueEvent event) {
                    receivedKeys.put(event.keyAsString(), event);
                }

                @Override
                public void onCommand(CommandEvent event) {
                    receivedCommands.add(event);
                }

                @Override
                public void onPsyncEnd(PsyncEndEvent event) {
                    latch.countDown();
                }

                @Override
                public void onError(Throwable t) {
                    t.printStackTrace();
                    latch.countDown();
                }
            })
            .build();

        Thread thread = snap.startAsync();
        latch.await(30, TimeUnit.SECONDS);
        snap.close();
        thread.join(5000);

        // Verify key has expiration
        StringKeyValue kv = (StringKeyValue) receivedKeys.get("expiring:key");
        assertThat(kv).isNotNull();
        assertThat(kv.hasExpiration()).isTrue();
        assertThat(kv.expireTimeMs()).isGreaterThan(System.currentTimeMillis());

        // Verify PEXPIREAT command was generated
        boolean hasPexpireat = receivedCommands.stream()
            .anyMatch(cmd -> cmd.command().name().equals("PEXPIREAT"));
        assertThat(hasPexpireat).isTrue();
    }

    @Test
    void replicateLargeList() throws Exception {
        // Create a large list to test chunking
        String[] values = new String[5000];
        for (int i = 0; i < 5000; i++) {
            values[i] = "value-" + i;
        }
        jedis.rpush("large:list", values);

        AtomicReference<ListKeyValue> receivedList = new AtomicReference<>();
        AtomicInteger commandCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);

        ValkeySnap snap = ValkeySnap.builder()
            .host(valkey.getHost())
            .port(valkey.getMappedPort(6379))
            .emitKeyValueEvents(true)
            .emitCommandEvents(true)
            .commandChunkMaxElements(1000)
            .listener(new SnapshotListenerAdapter() {
                @Override
                public void onKeyValue(KeyValueEvent event) {
                    if (event instanceof ListKeyValue lkv) {
                        receivedList.set(lkv);
                    }
                }

                @Override
                public void onCommand(CommandEvent event) {
                    if (event.command().name().equals("RPUSH")) {
                        commandCount.incrementAndGet();
                    }
                }

                @Override
                public void onPsyncEnd(PsyncEndEvent event) {
                    latch.countDown();
                }

                @Override
                public void onError(Throwable t) {
                    t.printStackTrace();
                    latch.countDown();
                }
            })
            .build();

        Thread thread = snap.startAsync();
        latch.await(60, TimeUnit.SECONDS);
        snap.close();
        thread.join(5000);

        // Verify all values received
        assertThat(receivedList.get()).isNotNull();
        assertThat(receivedList.get().size()).isEqualTo(5000);

        // Verify chunking: 5000 elements with max 1000 per chunk = 5 RPUSH commands
        assertThat(commandCount.get()).isEqualTo(5);
    }

    @Test
    void replicateMultipleDatabases() throws Exception {
        // Set data in different databases
        jedis.select(0);
        jedis.set("db0:key", "value0");

        jedis.select(1);
        jedis.set("db1:key", "value1");

        jedis.select(2);
        jedis.set("db2:key", "value2");

        Map<String, Integer> keyToDb = new ConcurrentHashMap<>();
        CountDownLatch latch = new CountDownLatch(1);

        ValkeySnap snap = ValkeySnap.builder()
            .host(valkey.getHost())
            .port(valkey.getMappedPort(6379))
            .emitKeyValueEvents(true)
            .listener(new SnapshotListenerAdapter() {
                @Override
                public void onKeyValue(KeyValueEvent event) {
                    keyToDb.put(event.keyAsString(), event.db());
                }

                @Override
                public void onPsyncEnd(PsyncEndEvent event) {
                    latch.countDown();
                }

                @Override
                public void onError(Throwable t) {
                    t.printStackTrace();
                    latch.countDown();
                }
            })
            .build();

        Thread thread = snap.startAsync();
        latch.await(30, TimeUnit.SECONDS);
        snap.close();
        thread.join(5000);

        // Verify keys are in correct databases
        assertThat(keyToDb).containsEntry("db0:key", 0);
        assertThat(keyToDb).containsEntry("db1:key", 1);
        assertThat(keyToDb).containsEntry("db2:key", 2);
    }

    private void setupTestData() {
        // STRING
        jedis.set("string:key", "hello world");

        // LIST
        jedis.rpush("list:key", "a", "b", "c", "d", "e");

        // SET
        jedis.sadd("set:key", "x", "y", "z");

        // ZSET (Sorted Set)
        jedis.zadd("zset:key", Map.of(
            "alice", 100.0,
            "bob", 90.0,
            "charlie", 80.0
        ));

        // HASH
        jedis.hset("hash:key", Map.of(
            "name", "John Doe",
            "age", "30",
            "city", "New York"
        ));
    }
}
