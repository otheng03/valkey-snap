package io.otheng.valkeysnap.protocol.resp;

/**
 * RESP (Redis Serialization Protocol) data types.
 */
public enum RespType {
    /**
     * Simple String: +OK\r\n
     */
    SIMPLE_STRING('+'),

    /**
     * Error: -ERR message\r\n
     */
    ERROR('-'),

    /**
     * Integer: :1000\r\n
     */
    INTEGER(':'),

    /**
     * Bulk String: $6\r\nfoobar\r\n
     */
    BULK_STRING('$'),

    /**
     * Array: *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
     */
    ARRAY('*'),

    /**
     * +CONTINUE\r\n[\n]
     * +FULLRESYNC 8de1787ba490483314a4d30f1c628bc5025eb761 2443808505[\n]$2443808505\r\nxxxxxxxxxxxxxxxx\r\n
     */
    LF('\n');

    private final char prefix;

    RespType(char prefix) {
        this.prefix = prefix;
    }

    public char getPrefix() {
        return prefix;
    }

    /**
     * Returns the RespType for the given prefix character.
     *
     * @param prefix the RESP type prefix character
     * @return the corresponding RespType
     * @throws IllegalArgumentException if prefix is not recognized
     */
    public static RespType fromPrefix(int prefix) {
        return switch (prefix) {
            case '+' -> SIMPLE_STRING;
            case '-' -> ERROR;
            case ':' -> INTEGER;
            case '$' -> BULK_STRING;
            case '*' -> ARRAY;
            case '\n' -> LF;
            default -> throw new IllegalArgumentException(
                "Unknown RESP type prefix: " + (char) prefix + " (0x" + Integer.toHexString(prefix) + ")"
            );
        };
    }
}
