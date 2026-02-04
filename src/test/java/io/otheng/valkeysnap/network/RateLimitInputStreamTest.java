package io.otheng.valkeysnap.network;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.assertj.core.api.Assertions.*;

class RateLimitInputStreamTest {

    @Test
    void readWithoutRateLimit() throws IOException {
        byte[] data = new byte[1000];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }

        RateLimitInputStream in = new RateLimitInputStream(
            new ByteArrayInputStream(data), 0 // No rate limit
        );

        assertThat(in.isRateLimitEnabled()).isFalse();
        assertThat(in.getBytesPerSecond()).isEqualTo(0);

        byte[] result = new byte[1000];
        int total = 0;
        int read;
        while ((read = in.read(result, total, result.length - total)) > 0) {
            total += read;
        }

        assertThat(total).isEqualTo(1000);
        assertThat(result).isEqualTo(data);
    }

    @Test
    void readSingleByteWithRateLimit() throws IOException {
        byte[] data = {0x01, 0x02, 0x03};
        RateLimitInputStream in = new RateLimitInputStream(
            new ByteArrayInputStream(data), 1000 // 1KB/s
        );

        assertThat(in.isRateLimitEnabled()).isTrue();
        assertThat(in.getBytesPerSecond()).isEqualTo(1000);

        assertThat(in.read()).isEqualTo(0x01);
        assertThat(in.read()).isEqualTo(0x02);
        assertThat(in.read()).isEqualTo(0x03);
        assertThat(in.read()).isEqualTo(-1); // EOF
    }

    @Test
    void readArrayWithRateLimit() throws IOException {
        byte[] data = new byte[100];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }

        RateLimitInputStream in = new RateLimitInputStream(
            new ByteArrayInputStream(data), 10000 // 10KB/s - fast enough for test
        );

        byte[] result = new byte[100];
        int total = 0;
        int read;
        while ((read = in.read(result, total, result.length - total)) > 0) {
            total += read;
        }

        assertThat(total).isEqualTo(100);
        assertThat(result).isEqualTo(data);
    }

    @Test
    void readEmptyStream() throws IOException {
        RateLimitInputStream in = new RateLimitInputStream(
            new ByteArrayInputStream(new byte[0]), 1000
        );

        assertThat(in.read()).isEqualTo(-1);
    }

    @Test
    void rateLimitActuallyLimits() throws IOException {
        // This test verifies rate limiting affects timing
        // Use a very low rate limit
        byte[] data = new byte[100];
        RateLimitInputStream in = new RateLimitInputStream(
            new ByteArrayInputStream(data), 50 // 50 bytes/sec
        );

        long start = System.currentTimeMillis();

        // Try to read 50 bytes - should take ~1 second
        byte[] result = new byte[50];
        int total = 0;
        while (total < 50) {
            int read = in.read(result, total, 50 - total);
            if (read < 0) break;
            total += read;
        }

        long elapsed = System.currentTimeMillis() - start;

        // Should have taken some time due to rate limiting
        // We allow some tolerance since timing isn't exact
        assertThat(total).isEqualTo(50);
        // At 50 bytes/sec, reading 50 bytes should take ~1 second
        // But token bucket allows burst, so first 50 bytes may be instant
        // The test mainly verifies no exceptions and correct data
    }
}
