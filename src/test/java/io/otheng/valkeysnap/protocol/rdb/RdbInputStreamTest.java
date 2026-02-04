package io.otheng.valkeysnap.protocol.rdb;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;

import static org.assertj.core.api.Assertions.*;

class RdbInputStreamTest {

    @Test
    void readByte() throws IOException {
        RdbInputStream in = streamOf(0x42, 0xFF);

        assertThat(in.readByte()).isEqualTo(0x42);
        assertThat(in.readByte()).isEqualTo(0xFF);
        assertThat(in.getOffset()).isEqualTo(2);
    }

    @Test
    void readByteThrowsOnEof() {
        RdbInputStream in = streamOf();

        assertThatThrownBy(in::readByte)
            .isInstanceOf(EOFException.class);
    }

    @Test
    void readUint8() throws IOException {
        RdbInputStream in = streamOf(0x00, 0x7F, 0x80, 0xFF);

        assertThat(in.readUint8()).isEqualTo(0);
        assertThat(in.readUint8()).isEqualTo(127);
        assertThat(in.readUint8()).isEqualTo(128);
        assertThat(in.readUint8()).isEqualTo(255);
    }

    @Test
    void readInt8() throws IOException {
        RdbInputStream in = streamOf(0x00, 0x7F, 0x80, 0xFF);

        assertThat(in.readInt8()).isEqualTo(0);
        assertThat(in.readInt8()).isEqualTo(127);
        assertThat(in.readInt8()).isEqualTo(-128);
        assertThat(in.readInt8()).isEqualTo(-1);
    }

    @Test
    void readUint16LE() throws IOException {
        RdbInputStream in = streamOf(0x01, 0x02, 0xFF, 0xFF);

        assertThat(in.readUint16LE()).isEqualTo(0x0201);
        assertThat(in.readUint16LE()).isEqualTo(0xFFFF);
    }

    @Test
    void readInt16LE() throws IOException {
        RdbInputStream in = streamOf(0x01, 0x00, 0xFF, 0xFF, 0x00, 0x80);

        assertThat(in.readInt16LE()).isEqualTo(1);
        assertThat(in.readInt16LE()).isEqualTo(-1);
        assertThat(in.readInt16LE()).isEqualTo(-32768);
    }

    @Test
    void readUint32LE() throws IOException {
        RdbInputStream in = streamOf(0x01, 0x02, 0x03, 0x04, 0xFF, 0xFF, 0xFF, 0xFF);

        assertThat(in.readUint32LE()).isEqualTo(0x04030201L);
        assertThat(in.readUint32LE()).isEqualTo(0xFFFFFFFFL);
    }

    @Test
    void readInt32LE() throws IOException {
        RdbInputStream in = streamOf(0x01, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF);

        assertThat(in.readInt32LE()).isEqualTo(1);
        assertThat(in.readInt32LE()).isEqualTo(-1);
    }

    @Test
    void readInt64LE() throws IOException {
        RdbInputStream in = streamOf(
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF
        );

        assertThat(in.readInt64LE()).isEqualTo(1L);
        assertThat(in.readInt64LE()).isEqualTo(-1L);
    }

    @Test
    void readBytes() throws IOException {
        RdbInputStream in = streamOf(0x01, 0x02, 0x03, 0x04, 0x05);

        byte[] result = in.readBytes(3);

        assertThat(result).containsExactly(0x01, 0x02, 0x03);
        assertThat(in.getOffset()).isEqualTo(3);
    }

    @Test
    void readFully() throws IOException {
        RdbInputStream in = streamOf(0x01, 0x02, 0x03);
        byte[] buffer = new byte[3];

        in.readFully(buffer);

        assertThat(buffer).containsExactly(0x01, 0x02, 0x03);
    }

    @Test
    void readFullyThrowsOnEof() {
        RdbInputStream in = streamOf(0x01, 0x02);
        byte[] buffer = new byte[5];

        assertThatThrownBy(() -> in.readFully(buffer))
            .isInstanceOf(EOFException.class);
    }

    @Test
    void readString() throws IOException {
        RdbInputStream in = streamOf('H', 'e', 'l', 'l', 'o');

        String result = in.readString(5);

        assertThat(result).isEqualTo("Hello");
    }

    @Test
    void skipBytes() throws IOException {
        RdbInputStream in = streamOf(0x01, 0x02, 0x03, 0x04, 0x05);

        in.skipBytes(3);

        assertThat(in.getOffset()).isEqualTo(3);
        assertThat(in.readByte()).isEqualTo(0x04);
    }

    @Test
    void offsetTracking() throws IOException {
        RdbInputStream in = streamOf(0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08);

        assertThat(in.getOffset()).isEqualTo(0);

        in.read();
        assertThat(in.getOffset()).isEqualTo(1);

        in.readBytes(2);
        assertThat(in.getOffset()).isEqualTo(3);

        in.skipBytes(2);
        assertThat(in.getOffset()).isEqualTo(5);

        byte[] buf = new byte[2];
        in.read(buf, 0, 2);
        assertThat(in.getOffset()).isEqualTo(7);
    }

    @Test
    void readDouble() throws IOException {
        // 3.14159 as IEEE 754 double, little-endian
        RdbInputStream in = streamOf(
            0x6E, 0x86, 0x1B, 0xF0, 0xF9, 0x21, 0x09, 0x40
        );

        double result = in.readDouble();

        assertThat(result).isCloseTo(3.14159, within(0.00001));
    }

    private RdbInputStream streamOf(int... bytes) {
        byte[] data = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            data[i] = (byte) bytes[i];
        }
        return new RdbInputStream(new ByteArrayInputStream(data));
    }
}
