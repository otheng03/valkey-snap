package io.otheng.valkeysnap.util;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.*;

class CRC64Test {

    @Test
    void computeEmpty() {
        CRC64 crc = new CRC64();
        assertThat(crc.getValue()).isEqualTo(0L);
    }

    @Test
    void computeSingleByte() {
        CRC64 crc = new CRC64();
        crc.update((byte) 0x01);
        assertThat(crc.getValue()).isNotEqualTo(0L);
    }

    @Test
    void computeString() {
        long crc = CRC64.compute("Hello".getBytes(StandardCharsets.UTF_8));
        assertThat(crc).isNotEqualTo(0L);
    }

    @Test
    void computeSameInputSameResult() {
        byte[] data = "test data".getBytes(StandardCharsets.UTF_8);

        long crc1 = CRC64.compute(data);
        long crc2 = CRC64.compute(data);

        assertThat(crc1).isEqualTo(crc2);
    }

    @Test
    void computeDifferentInputDifferentResult() {
        long crc1 = CRC64.compute("hello".getBytes(StandardCharsets.UTF_8));
        long crc2 = CRC64.compute("world".getBytes(StandardCharsets.UTF_8));

        assertThat(crc1).isNotEqualTo(crc2);
    }

    @Test
    void incrementalUpdate() {
        byte[] data = "HelloWorld".getBytes(StandardCharsets.UTF_8);

        // Compute all at once
        long crcAll = CRC64.compute(data);

        // Compute incrementally
        CRC64 crc = new CRC64();
        crc.update("Hello".getBytes(StandardCharsets.UTF_8));
        crc.update("World".getBytes(StandardCharsets.UTF_8));

        assertThat(crc.getValue()).isEqualTo(crcAll);
    }

    @Test
    void toBytesLittleEndian() {
        CRC64 crc = new CRC64();
        crc.update("test".getBytes(StandardCharsets.UTF_8));

        byte[] bytes = crc.toBytes();

        assertThat(bytes).hasSize(8);

        // Verify it's little-endian by reconstructing the value
        long reconstructed = 0;
        for (int i = 7; i >= 0; i--) {
            reconstructed = (reconstructed << 8) | (bytes[i] & 0xFF);
        }
        assertThat(reconstructed).isEqualTo(crc.getValue());
    }

    @Test
    void reset() {
        CRC64 crc = new CRC64();
        crc.update("data".getBytes(StandardCharsets.UTF_8));
        long beforeReset = crc.getValue();

        crc.reset();

        assertThat(crc.getValue()).isEqualTo(0L);
        assertThat(crc.getValue()).isNotEqualTo(beforeReset);
    }

    @Test
    void computeBytesStatic() {
        byte[] data = "test".getBytes(StandardCharsets.UTF_8);
        byte[] crcBytes = CRC64.computeBytes(data);

        assertThat(crcBytes).hasSize(8);

        // Should match instance method
        CRC64 crc = new CRC64();
        crc.update(data);
        assertThat(crcBytes).isEqualTo(crc.toBytes());
    }

    @Test
    void updateWithOffset() {
        byte[] data = "XXXtestXXX".getBytes(StandardCharsets.UTF_8);

        CRC64 crc1 = new CRC64();
        crc1.update("test".getBytes(StandardCharsets.UTF_8));

        CRC64 crc2 = new CRC64();
        crc2.update(data, 3, 4);

        assertThat(crc2.getValue()).isEqualTo(crc1.getValue());
    }
}
