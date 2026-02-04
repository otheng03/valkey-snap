package io.otheng.valkeysnap.protocol.resp;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

class RespParserTest {

    @Test
    void parseSimpleString() throws IOException {
        RespParser parser = parserFor("+OK\r\n");
        RespValue result = parser.parse();

        assertThat(result).isInstanceOf(RespValue.SimpleString.class);
        assertThat(((RespValue.SimpleString) result).value()).isEqualTo("OK");
    }

    @Test
    void parseError() throws IOException {
        RespParser parser = parserFor("-ERR unknown command\r\n");
        RespValue result = parser.parse();

        assertThat(result).isInstanceOf(RespValue.Error.class);
        assertThat(((RespValue.Error) result).message()).isEqualTo("ERR unknown command");
    }

    @Test
    void parseInteger() throws IOException {
        RespParser parser = parserFor(":1000\r\n");
        RespValue result = parser.parse();

        assertThat(result).isInstanceOf(RespValue.Integer.class);
        assertThat(((RespValue.Integer) result).value()).isEqualTo(1000L);
    }

    @Test
    void parseNegativeInteger() throws IOException {
        RespParser parser = parserFor(":-1\r\n");
        RespValue result = parser.parse();

        assertThat(result).isInstanceOf(RespValue.Integer.class);
        assertThat(((RespValue.Integer) result).value()).isEqualTo(-1L);
    }

    @Test
    void parseBulkString() throws IOException {
        RespParser parser = parserFor("$6\r\nfoobar\r\n");
        RespValue result = parser.parse();

        assertThat(result).isInstanceOf(RespValue.BulkString.class);
        RespValue.BulkString bs = (RespValue.BulkString) result;
        assertThat(bs.asString()).isEqualTo("foobar");
        assertThat(bs.isNull()).isFalse();
    }

    @Test
    void parseNullBulkString() throws IOException {
        RespParser parser = parserFor("$-1\r\n");
        RespValue result = parser.parse();

        assertThat(result).isInstanceOf(RespValue.BulkString.class);
        RespValue.BulkString bs = (RespValue.BulkString) result;
        assertThat(bs.isNull()).isTrue();
        assertThat(bs.asString()).isNull();
    }

    @Test
    void parseEmptyBulkString() throws IOException {
        RespParser parser = parserFor("$0\r\n\r\n");
        RespValue result = parser.parse();

        assertThat(result).isInstanceOf(RespValue.BulkString.class);
        RespValue.BulkString bs = (RespValue.BulkString) result;
        assertThat(bs.asString()).isEmpty();
        assertThat(bs.isNull()).isFalse();
    }

    @Test
    void parseArray() throws IOException {
        RespParser parser = parserFor("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        RespValue result = parser.parse();

        assertThat(result).isInstanceOf(RespValue.Array.class);
        RespValue.Array arr = (RespValue.Array) result;
        assertThat(arr.size()).isEqualTo(2);
        assertThat(arr.isNull()).isFalse();

        assertThat(arr.get(0)).isInstanceOf(RespValue.BulkString.class);
        assertThat(((RespValue.BulkString) arr.get(0)).asString()).isEqualTo("foo");

        assertThat(arr.get(1)).isInstanceOf(RespValue.BulkString.class);
        assertThat(((RespValue.BulkString) arr.get(1)).asString()).isEqualTo("bar");
    }

    @Test
    void parseNullArray() throws IOException {
        RespParser parser = parserFor("*-1\r\n");
        RespValue result = parser.parse();

        assertThat(result).isInstanceOf(RespValue.Array.class);
        RespValue.Array arr = (RespValue.Array) result;
        assertThat(arr.isNull()).isTrue();
        assertThat(arr.size()).isEqualTo(-1);
    }

    @Test
    void parseEmptyArray() throws IOException {
        RespParser parser = parserFor("*0\r\n");
        RespValue result = parser.parse();

        assertThat(result).isInstanceOf(RespValue.Array.class);
        RespValue.Array arr = (RespValue.Array) result;
        assertThat(arr.size()).isEqualTo(0);
        assertThat(arr.isNull()).isFalse();
    }

    @Test
    void parseMixedArray() throws IOException {
        RespParser parser = parserFor("*3\r\n:1\r\n+OK\r\n$4\r\ntest\r\n");
        RespValue result = parser.parse();

        assertThat(result).isInstanceOf(RespValue.Array.class);
        RespValue.Array arr = (RespValue.Array) result;
        assertThat(arr.size()).isEqualTo(3);

        assertThat(arr.get(0)).isInstanceOf(RespValue.Integer.class);
        assertThat(arr.get(1)).isInstanceOf(RespValue.SimpleString.class);
        assertThat(arr.get(2)).isInstanceOf(RespValue.BulkString.class);
    }

    @Test
    void parseNestedArray() throws IOException {
        RespParser parser = parserFor("*2\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n*1\r\n$1\r\nc\r\n");
        RespValue result = parser.parse();

        assertThat(result).isInstanceOf(RespValue.Array.class);
        RespValue.Array outer = (RespValue.Array) result;
        assertThat(outer.size()).isEqualTo(2);

        RespValue.Array inner1 = (RespValue.Array) outer.get(0);
        assertThat(inner1.size()).isEqualTo(2);

        RespValue.Array inner2 = (RespValue.Array) outer.get(1);
        assertThat(inner2.size()).isEqualTo(1);
    }

    @Test
    void parseMultipleValues() throws IOException {
        RespParser parser = parserFor("+PONG\r\n+OK\r\n:42\r\n");

        RespValue first = parser.parse();
        assertThat(((RespValue.SimpleString) first).value()).isEqualTo("PONG");

        RespValue second = parser.parse();
        assertThat(((RespValue.SimpleString) second).value()).isEqualTo("OK");

        RespValue third = parser.parse();
        assertThat(((RespValue.Integer) third).value()).isEqualTo(42L);
    }

    private RespParser parserFor(String data) {
        return new RespParser(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8)));
    }
}
