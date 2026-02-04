package io.otheng.valkeysnap.config;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.*;

class SnapshotConfigTest {

    @Test
    void defaultValues() {
        SnapshotConfig config = SnapshotConfig.builder()
            .host("localhost")
            .build();

        assertThat(config.host()).isEqualTo("localhost");
        assertThat(config.port()).isEqualTo(6379);
        assertThat(config.password()).isNull();
        assertThat(config.connectionTimeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(config.readTimeout()).isEqualTo(Duration.ofSeconds(60));
        assertThat(config.rateLimitBytesPerSecond()).isEqualTo(0);
        assertThat(config.emitKeyValueEvents()).isTrue();
        assertThat(config.emitCommandEvents()).isFalse();
        assertThat(config.emitRawBytes()).isFalse();
        assertThat(config.commandChunkMaxElements()).isEqualTo(1000);
        assertThat(config.commandChunkMaxBytes()).isEqualTo(64 * 1024);
        assertThat(config.replId()).isEqualTo("?");
        assertThat(config.replOffset()).isEqualTo(-1);
    }

    @Test
    void customValues() {
        SnapshotConfig config = SnapshotConfig.builder()
            .host("redis.example.com")
            .port(6380)
            .password("secret")
            .connectionTimeout(Duration.ofSeconds(10))
            .readTimeout(Duration.ofMinutes(5))
            .rateLimitBytesPerSecond(1024 * 1024)
            .emitKeyValueEvents(false)
            .emitCommandEvents(true)
            .emitRawBytes(true)
            .commandChunkMaxElements(500)
            .commandChunkMaxBytes(32 * 1024)
            .replId("abc123")
            .replOffset(12345)
            .build();

        assertThat(config.host()).isEqualTo("redis.example.com");
        assertThat(config.port()).isEqualTo(6380);
        assertThat(config.password()).isEqualTo("secret");
        assertThat(config.connectionTimeout()).isEqualTo(Duration.ofSeconds(10));
        assertThat(config.readTimeout()).isEqualTo(Duration.ofMinutes(5));
        assertThat(config.rateLimitBytesPerSecond()).isEqualTo(1024 * 1024);
        assertThat(config.emitKeyValueEvents()).isFalse();
        assertThat(config.emitCommandEvents()).isTrue();
        assertThat(config.emitRawBytes()).isTrue();
        assertThat(config.commandChunkMaxElements()).isEqualTo(500);
        assertThat(config.commandChunkMaxBytes()).isEqualTo(32 * 1024);
        assertThat(config.replId()).isEqualTo("abc123");
        assertThat(config.replOffset()).isEqualTo(12345);
    }

    @Test
    void invalidPort() {
        assertThatThrownBy(() -> SnapshotConfig.builder().port(0))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Port");

        assertThatThrownBy(() -> SnapshotConfig.builder().port(70000))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Port");
    }

    @Test
    void invalidRateLimit() {
        assertThatThrownBy(() -> SnapshotConfig.builder().rateLimitBytesPerSecond(-1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Rate limit");
    }

    @Test
    void invalidChunkMaxElements() {
        assertThatThrownBy(() -> SnapshotConfig.builder().commandChunkMaxElements(0))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Max elements");
    }

    @Test
    void invalidChunkMaxBytes() {
        assertThatThrownBy(() -> SnapshotConfig.builder().commandChunkMaxBytes(0))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Max bytes");
    }

    @Test
    void nullHost() {
        assertThatThrownBy(() -> SnapshotConfig.builder().host(null).build())
            .isInstanceOf(NullPointerException.class);
    }

    @Test
    void nullTimeout() {
        assertThatThrownBy(() -> SnapshotConfig.builder().connectionTimeout(null))
            .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> SnapshotConfig.builder().readTimeout(null))
            .isInstanceOf(NullPointerException.class);
    }
}
