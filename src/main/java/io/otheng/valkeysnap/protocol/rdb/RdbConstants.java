package io.otheng.valkeysnap.protocol.rdb;

/**
 * Constants for RDB file format parsing.
 */
public final class RdbConstants {

    private RdbConstants() {}

    // RDB file signature
    public static final byte[] MAGIC = "REDIS".getBytes();
    public static final int RDB_VERSION_MIN = 1;
    public static final int RDB_VERSION_MAX = 11;

    // Opcodes
    public static final int RDB_OPCODE_MODULE_AUX = 247;
    public static final int RDB_OPCODE_IDLE = 248;
    public static final int RDB_OPCODE_FREQ = 249;
    public static final int RDB_OPCODE_AUX = 250;
    public static final int RDB_OPCODE_RESIZEDB = 251;
    public static final int RDB_OPCODE_EXPIRETIME_MS = 252;
    public static final int RDB_OPCODE_EXPIRETIME = 253;
    public static final int RDB_OPCODE_SELECTDB = 254;
    public static final int RDB_OPCODE_EOF = 255;

    // Value types
    public static final int RDB_TYPE_STRING = 0;
    public static final int RDB_TYPE_LIST = 1;
    public static final int RDB_TYPE_SET = 2;
    public static final int RDB_TYPE_ZSET = 3;
    public static final int RDB_TYPE_HASH = 4;
    public static final int RDB_TYPE_ZSET_2 = 5; // ZSET with double scores
    public static final int RDB_TYPE_MODULE = 6;
    public static final int RDB_TYPE_MODULE_2 = 7;

    // Encoded value types (ziplist, intset, etc.)
    public static final int RDB_TYPE_HASH_ZIPMAP = 9;
    public static final int RDB_TYPE_LIST_ZIPLIST = 10;
    public static final int RDB_TYPE_SET_INTSET = 11;
    public static final int RDB_TYPE_ZSET_ZIPLIST = 12;
    public static final int RDB_TYPE_HASH_ZIPLIST = 13;
    public static final int RDB_TYPE_LIST_QUICKLIST = 14;
    public static final int RDB_TYPE_STREAM_LISTPACKS = 15;
    public static final int RDB_TYPE_HASH_LISTPACK = 16;
    public static final int RDB_TYPE_ZSET_LISTPACK = 17;
    public static final int RDB_TYPE_LIST_QUICKLIST_2 = 18;
    public static final int RDB_TYPE_STREAM_LISTPACKS_2 = 19;
    public static final int RDB_TYPE_SET_LISTPACK = 20;
    public static final int RDB_TYPE_STREAM_LISTPACKS_3 = 21;

    // Length encoding types (2 high bits of first byte)
    public static final int RDB_6BITLEN = 0;    // 00xxxxxx - 6 bit length
    public static final int RDB_14BITLEN = 1;   // 01xxxxxx xxxxxxxx - 14 bit length
    public static final int RDB_32BITLEN = 2;   // 10000000 - 32 bit length follows
    public static final int RDB_ENCVAL = 3;     // 11xxxxxx - special encoding

    // Special encoding types (when RDB_ENCVAL)
    public static final int RDB_ENC_INT8 = 0;   // 8 bit signed integer
    public static final int RDB_ENC_INT16 = 1;  // 16 bit signed integer
    public static final int RDB_ENC_INT32 = 2;  // 32 bit signed integer
    public static final int RDB_ENC_LZF = 3;    // LZF compressed string

    // Extended length encoding (0x80)
    public static final int RDB_32BITLEN_MARKER = 0x80;
    public static final int RDB_64BITLEN_MARKER = 0x81;

    /**
     * Returns a human-readable name for the given RDB type.
     */
    public static String typeName(int type) {
        return switch (type) {
            case RDB_TYPE_STRING -> "STRING";
            case RDB_TYPE_LIST -> "LIST";
            case RDB_TYPE_SET -> "SET";
            case RDB_TYPE_ZSET -> "ZSET";
            case RDB_TYPE_HASH -> "HASH";
            case RDB_TYPE_ZSET_2 -> "ZSET_2";
            case RDB_TYPE_MODULE -> "MODULE";
            case RDB_TYPE_MODULE_2 -> "MODULE_2";
            case RDB_TYPE_HASH_ZIPMAP -> "HASH_ZIPMAP";
            case RDB_TYPE_LIST_ZIPLIST -> "LIST_ZIPLIST";
            case RDB_TYPE_SET_INTSET -> "SET_INTSET";
            case RDB_TYPE_ZSET_ZIPLIST -> "ZSET_ZIPLIST";
            case RDB_TYPE_HASH_ZIPLIST -> "HASH_ZIPLIST";
            case RDB_TYPE_LIST_QUICKLIST -> "LIST_QUICKLIST";
            case RDB_TYPE_STREAM_LISTPACKS -> "STREAM_LISTPACKS";
            case RDB_TYPE_HASH_LISTPACK -> "HASH_LISTPACK";
            case RDB_TYPE_ZSET_LISTPACK -> "ZSET_LISTPACK";
            case RDB_TYPE_LIST_QUICKLIST_2 -> "LIST_QUICKLIST_2";
            case RDB_TYPE_STREAM_LISTPACKS_2 -> "STREAM_LISTPACKS_2";
            case RDB_TYPE_SET_LISTPACK -> "SET_LISTPACK";
            case RDB_TYPE_STREAM_LISTPACKS_3 -> "STREAM_LISTPACKS_3";
            default -> "UNKNOWN(" + type + ")";
        };
    }

    /**
     * Returns true if the type is a variant that stores strings.
     */
    public static boolean isStringType(int type) {
        return type == RDB_TYPE_STRING;
    }

    /**
     * Returns true if the type is a variant that stores lists.
     */
    public static boolean isListType(int type) {
        return type == RDB_TYPE_LIST ||
               type == RDB_TYPE_LIST_ZIPLIST ||
               type == RDB_TYPE_LIST_QUICKLIST ||
               type == RDB_TYPE_LIST_QUICKLIST_2;
    }

    /**
     * Returns true if the type is a variant that stores sets.
     */
    public static boolean isSetType(int type) {
        return type == RDB_TYPE_SET ||
               type == RDB_TYPE_SET_INTSET ||
               type == RDB_TYPE_SET_LISTPACK;
    }

    /**
     * Returns true if the type is a variant that stores sorted sets.
     */
    public static boolean isZSetType(int type) {
        return type == RDB_TYPE_ZSET ||
               type == RDB_TYPE_ZSET_2 ||
               type == RDB_TYPE_ZSET_ZIPLIST ||
               type == RDB_TYPE_ZSET_LISTPACK;
    }

    /**
     * Returns true if the type is a variant that stores hashes.
     */
    public static boolean isHashType(int type) {
        return type == RDB_TYPE_HASH ||
               type == RDB_TYPE_HASH_ZIPMAP ||
               type == RDB_TYPE_HASH_ZIPLIST ||
               type == RDB_TYPE_HASH_LISTPACK;
    }

    /**
     * Returns true if the type is a stream variant.
     */
    public static boolean isStreamType(int type) {
        return type == RDB_TYPE_STREAM_LISTPACKS ||
               type == RDB_TYPE_STREAM_LISTPACKS_2 ||
               type == RDB_TYPE_STREAM_LISTPACKS_3;
    }

    /**
     * Returns true if the type is a module variant.
     */
    public static boolean isModuleType(int type) {
        return type == RDB_TYPE_MODULE || type == RDB_TYPE_MODULE_2;
    }
}
