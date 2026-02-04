package io.otheng.valkeysnap.protocol.rdb;

import io.otheng.valkeysnap.util.ByteUtils;
import io.otheng.valkeysnap.util.LzfDecompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static io.otheng.valkeysnap.protocol.rdb.RdbConstants.*;

/**
 * Parser for Redis RDB format.
 * Parses the binary RDB stream and invokes callbacks on the visitor.
 */
public class RdbParser {

    private static final Logger logger = LoggerFactory.getLogger(RdbParser.class);

    private final RdbInputStream in;
    private final RdbVisitor visitor;
    private int rdbVersion;
    private long currentExpireTime = -1;

    public RdbParser(RdbInputStream in, RdbVisitor visitor) {
        this.in = in;
        this.visitor = visitor;
    }

    /**
     * Parses the RDB stream from beginning to end.
     *
     * @throws IOException if parsing fails
     */
    public void parse() throws IOException {
        try {
            parseHeader();
            visitor.onRdbStart(rdbVersion);

            parseBody();
        } catch (Exception e) {
            visitor.onError(e);
            throw e;
        }
    }

    private void parseHeader() throws IOException {
        // Read magic "REDIS"
        byte[] magic = in.readBytes(5);
        if (!Arrays.equals(magic, MAGIC)) {
            throw new RdbParseException("Invalid RDB magic: " + new String(magic, StandardCharsets.US_ASCII));
        }

        // Read version (4 bytes ASCII, e.g., "0011")
        byte[] versionBytes = in.readBytes(4);
        String versionStr = new String(versionBytes, StandardCharsets.US_ASCII);
        try {
            rdbVersion = Integer.parseInt(versionStr);
        } catch (NumberFormatException e) {
            throw new RdbParseException("Invalid RDB version: " + versionStr);
        }

        if (rdbVersion < RDB_VERSION_MIN || rdbVersion > RDB_VERSION_MAX) {
            logger.warn("RDB version {} may not be fully supported (supported: {}-{})",
                    rdbVersion, RDB_VERSION_MIN, RDB_VERSION_MAX);
        }

        logger.debug("RDB version: {}", rdbVersion);
    }

    private void parseBody() throws IOException {
        int currentDb = 0;

        while (true) {
            int type = in.readUint8();

            switch (type) {
                case RDB_OPCODE_EOF -> {
                    byte[] checksum = in.readBytes(8);
                    visitor.onRdbEnd(checksum);
                    return;
                }
                case RDB_OPCODE_SELECTDB -> {
                    currentDb = (int) readLength();
                    visitor.onSelectDb(currentDb);
                    logger.debug("Selected DB: {}", currentDb);
                }
                case RDB_OPCODE_RESIZEDB -> {
                    long dbSize = readLength();
                    long expiresSize = readLength();
                    visitor.onResizeDb(dbSize, expiresSize);
                }
                case RDB_OPCODE_EXPIRETIME -> {
                    // Expire time in seconds (4 bytes)
                    currentExpireTime = in.readUint32LE() * 1000;
                }
                case RDB_OPCODE_EXPIRETIME_MS -> {
                    // Expire time in milliseconds (8 bytes)
                    currentExpireTime = in.readInt64LE();
                }
                case RDB_OPCODE_AUX -> {
                    String key = ByteUtils.toString(readString());
                    String value = ByteUtils.toString(readString());
                    visitor.onAuxField(key, value);
                    logger.debug("AUX: {}={}", key, value);
                }
                case RDB_OPCODE_FREQ -> {
                    // LFU frequency - just skip
                    in.readUint8();
                }
                case RDB_OPCODE_IDLE -> {
                    // LRU idle time - just skip
                    readLength();
                }
                case RDB_OPCODE_MODULE_AUX -> {
                    // Module auxiliary data - skip
                    skipModuleData();
                }
                default -> {
                    // It's a key-value type
                    parseKeyValue(type);
                    currentExpireTime = -1; // Reset after use
                }
            }
        }
    }

    private void parseKeyValue(int type) throws IOException {
        byte[] key = readString();

        switch (type) {
            case RDB_TYPE_STRING -> parseString(key);
            case RDB_TYPE_LIST -> parseList(key);
            case RDB_TYPE_SET -> parseSet(key);
            case RDB_TYPE_ZSET -> parseSortedSet(key, false);
            case RDB_TYPE_ZSET_2 -> parseSortedSet(key, true);
            case RDB_TYPE_HASH -> parseHash(key);
            case RDB_TYPE_LIST_ZIPLIST -> parseListZiplist(key);
            case RDB_TYPE_SET_INTSET -> parseSetIntset(key);
            case RDB_TYPE_ZSET_ZIPLIST -> parseSortedSetZiplist(key);
            case RDB_TYPE_HASH_ZIPLIST -> parseHashZiplist(key);
            case RDB_TYPE_LIST_QUICKLIST -> parseListQuicklist(key);
            case RDB_TYPE_LIST_QUICKLIST_2 -> parseListQuicklist2(key);
            case RDB_TYPE_HASH_ZIPMAP -> parseHashZipmap(key);
            case RDB_TYPE_HASH_LISTPACK -> parseHashListpack(key);
            case RDB_TYPE_ZSET_LISTPACK -> parseSortedSetListpack(key);
            case RDB_TYPE_SET_LISTPACK -> parseSetListpack(key);
            case RDB_TYPE_STREAM_LISTPACKS, RDB_TYPE_STREAM_LISTPACKS_2, RDB_TYPE_STREAM_LISTPACKS_3 ->
                parseStream(key, type);
            case RDB_TYPE_MODULE, RDB_TYPE_MODULE_2 -> parseModule(key, type);
            default -> throw new RdbParseException("Unknown RDB type: " + type);
        }
    }

    // ==================== Length Encoding ====================

    /**
     * Reads a length-encoded value.
     * Returns the length, or negative value for special encodings.
     */
    private long readLength() throws IOException {
        int first = in.readUint8();
        int type = (first & 0xC0) >> 6;

        return switch (type) {
            case RDB_6BITLEN -> first & 0x3F;
            case RDB_14BITLEN -> ((first & 0x3F) << 8) | in.readUint8();
            case RDB_32BITLEN -> {
                if (first == RDB_32BITLEN_MARKER) {
                    yield in.readUint32BE();
                } else if (first == RDB_64BITLEN_MARKER) {
                    yield in.readInt64LE();
                } else {
                    yield in.readUint32BE();
                }
            }
            case RDB_ENCVAL -> -(first & 0x3F) - 1; // Return negative to signal special encoding
            default -> throw new RdbParseException("Invalid length encoding type: " + type);
        };
    }

    /**
     * Reads a length-encoded string.
     */
    private byte[] readString() throws IOException {
        int first = in.readUint8();
        int type = (first & 0xC0) >> 6;

        if (type == RDB_ENCVAL) {
            int encType = first & 0x3F;
            return readEncodedString(encType);
        }

        // Regular string with length prefix
        long length;
        if (type == RDB_6BITLEN) {
            length = first & 0x3F;
        } else if (type == RDB_14BITLEN) {
            length = ((first & 0x3F) << 8) | in.readUint8();
        } else if (type == RDB_32BITLEN) {
            if (first == RDB_32BITLEN_MARKER) {
                length = in.readUint32BE();
            } else if (first == RDB_64BITLEN_MARKER) {
                length = in.readInt64LE();
            } else {
                length = in.readUint32BE();
            }
        } else {
            throw new RdbParseException("Invalid string length encoding");
        }

        return in.readBytes((int) length);
    }

    private byte[] readEncodedString(int encType) throws IOException {
        return switch (encType) {
            case RDB_ENC_INT8 -> String.valueOf(in.readInt8()).getBytes(StandardCharsets.UTF_8);
            case RDB_ENC_INT16 -> String.valueOf(in.readInt16LE()).getBytes(StandardCharsets.UTF_8);
            case RDB_ENC_INT32 -> String.valueOf(in.readInt32LE()).getBytes(StandardCharsets.UTF_8);
            case RDB_ENC_LZF -> {
                int compressedLen = (int) readLength();
                int uncompressedLen = (int) readLength();
                byte[] compressed = in.readBytes(compressedLen);
                yield LzfDecompressor.decompress(compressed, uncompressedLen);
            }
            default -> throw new RdbParseException("Unknown string encoding: " + encType);
        };
    }

    // ==================== Type Parsers ====================

    private void parseString(byte[] key) throws IOException {
        byte[] value = readString();
        visitor.onString(key, value, currentExpireTime);
    }

    private void parseList(byte[] key) throws IOException {
        long size = readLength();
        List<byte[]> values = new ArrayList<>((int) Math.min(size, 10000));
        for (long i = 0; i < size; i++) {
            values.add(readString());
        }
        visitor.onList(key, values, currentExpireTime);
    }

    private void parseSet(byte[] key) throws IOException {
        long size = readLength();
        List<byte[]> members = new ArrayList<>((int) Math.min(size, 10000));
        for (long i = 0; i < size; i++) {
            members.add(readString());
        }
        visitor.onSet(key, members, currentExpireTime);
    }

    private void parseSortedSet(byte[] key, boolean version2) throws IOException {
        long size = readLength();
        Map<byte[], Double> entries = new LinkedHashMap<>();
        for (long i = 0; i < size; i++) {
            byte[] member = readString();
            double score;
            if (version2) {
                score = in.readDouble();
            } else {
                score = readDoubleString();
            }
            entries.put(member, score);
        }
        visitor.onSortedSet(key, entries, currentExpireTime);
    }

    private double readDoubleString() throws IOException {
        int len = in.readUint8();
        if (len == 255) {
            return Double.NEGATIVE_INFINITY;
        } else if (len == 254) {
            return Double.POSITIVE_INFINITY;
        } else if (len == 253) {
            return Double.NaN;
        } else {
            byte[] bytes = in.readBytes(len);
            return Double.parseDouble(new String(bytes, StandardCharsets.US_ASCII));
        }
    }

    private void parseHash(byte[] key) throws IOException {
        long size = readLength();
        Map<byte[], byte[]> fields = new LinkedHashMap<>();
        for (long i = 0; i < size; i++) {
            byte[] field = readString();
            byte[] value = readString();
            fields.put(field, value);
        }
        visitor.onHash(key, fields, currentExpireTime);
    }

    // ==================== Ziplist Parsers ====================

    private void parseListZiplist(byte[] key) throws IOException {
        byte[] ziplist = readString();
        List<byte[]> values = parseZiplistEntries(ziplist);
        visitor.onList(key, values, currentExpireTime);
    }

    private void parseSetIntset(byte[] key) throws IOException {
        byte[] data = readString();
        List<byte[]> members = parseIntset(data);
        visitor.onSet(key, members, currentExpireTime);
    }

    private List<byte[]> parseIntset(byte[] data) {
        int encoding = ByteUtils.readUint16LE(data, 0);
        int size = (int) ByteUtils.readUint32LE(data, 2);
        List<byte[]> members = new ArrayList<>(size);

        int offset = 6;
        for (int i = 0; i < size; i++) {
            long value = switch (encoding) {
                case 2 -> (short) ByteUtils.readUint16LE(data, offset);
                case 4 -> (int) ByteUtils.readUint32LE(data, offset);
                case 8 -> ByteUtils.readInt64LE(data, offset);
                default -> throw new RdbParseException("Unknown intset encoding: " + encoding);
            };
            members.add(String.valueOf(value).getBytes(StandardCharsets.UTF_8));
            offset += encoding;
        }
        return members;
    }

    private void parseSortedSetZiplist(byte[] key) throws IOException {
        byte[] ziplist = readString();
        List<byte[]> entries = parseZiplistEntries(ziplist);

        Map<byte[], Double> result = new LinkedHashMap<>();
        for (int i = 0; i < entries.size(); i += 2) {
            byte[] member = entries.get(i);
            byte[] scoreBytes = entries.get(i + 1);
            double score = Double.parseDouble(new String(scoreBytes, StandardCharsets.UTF_8));
            result.put(member, score);
        }
        visitor.onSortedSet(key, result, currentExpireTime);
    }

    private void parseHashZiplist(byte[] key) throws IOException {
        byte[] ziplist = readString();
        List<byte[]> entries = parseZiplistEntries(ziplist);

        Map<byte[], byte[]> fields = new LinkedHashMap<>();
        for (int i = 0; i < entries.size(); i += 2) {
            fields.put(entries.get(i), entries.get(i + 1));
        }
        visitor.onHash(key, fields, currentExpireTime);
    }

    private List<byte[]> parseZiplistEntries(byte[] ziplist) {
        // Ziplist format:
        // <zlbytes><zltail><zllen><entry>...<entry><zlend>
        // zlbytes: 4 bytes (total bytes)
        // zltail: 4 bytes (offset to last entry)
        // zllen: 2 bytes (number of entries, or 65535 if > 65534)
        // zlend: 1 byte (0xFF)

        int offset = 0;
        // long zlbytes = ByteUtils.readUint32LE(ziplist, offset);
        offset += 4;
        // long zltail = ByteUtils.readUint32LE(ziplist, offset);
        offset += 4;
        int zllen = ByteUtils.readUint16LE(ziplist, offset);
        offset += 2;

        List<byte[]> entries = new ArrayList<>(zllen == 65535 ? 100 : zllen);

        while (offset < ziplist.length - 1) { // -1 for zlend
            if ((ziplist[offset] & 0xFF) == 0xFF) {
                break; // zlend
            }

            // Previous entry length
            int prevLen = ziplist[offset++] & 0xFF;
            if (prevLen == 254) {
                // 4-byte length follows
                offset += 4;
            }

            // Entry encoding
            int encoding = ziplist[offset] & 0xFF;
            byte[] value;

            if ((encoding & 0xC0) == 0) {
                // 6-bit string length
                int len = encoding & 0x3F;
                offset++;
                value = Arrays.copyOfRange(ziplist, offset, offset + len);
                offset += len;
            } else if ((encoding & 0xC0) == 0x40) {
                // 14-bit string length
                int len = ((encoding & 0x3F) << 8) | (ziplist[offset + 1] & 0xFF);
                offset += 2;
                value = Arrays.copyOfRange(ziplist, offset, offset + len);
                offset += len;
            } else if ((encoding & 0xC0) == 0x80) {
                // 32-bit string length
                offset++;
                int len = (int) ByteUtils.readUint32LE(ziplist, offset);
                offset += 4;
                value = Arrays.copyOfRange(ziplist, offset, offset + len);
                offset += len;
            } else {
                // Integer encoding
                long intVal;
                if (encoding == 0xC0) {
                    // int16
                    offset++;
                    intVal = (short) ByteUtils.readUint16LE(ziplist, offset);
                    offset += 2;
                } else if (encoding == 0xD0) {
                    // int32
                    offset++;
                    intVal = (int) ByteUtils.readUint32LE(ziplist, offset);
                    offset += 4;
                } else if (encoding == 0xE0) {
                    // int64
                    offset++;
                    intVal = ByteUtils.readInt64LE(ziplist, offset);
                    offset += 8;
                } else if (encoding == 0xF0) {
                    // 24-bit signed integer
                    offset++;
                    intVal = (ziplist[offset] & 0xFF) |
                            ((ziplist[offset + 1] & 0xFF) << 8) |
                            ((ziplist[offset + 2] & 0xFF) << 16);
                    if ((intVal & 0x800000) != 0) {
                        intVal |= 0xFF000000L; // Sign extend
                    }
                    offset += 3;
                } else if (encoding == 0xFE) {
                    // 8-bit signed integer
                    offset++;
                    intVal = (byte) ziplist[offset];
                    offset++;
                } else if ((encoding & 0xF0) == 0xF0) {
                    // 4-bit immediate (0-12)
                    intVal = (encoding & 0x0F) - 1;
                    offset++;
                } else {
                    throw new RdbParseException("Unknown ziplist encoding: " + encoding);
                }
                value = String.valueOf(intVal).getBytes(StandardCharsets.UTF_8);
            }
            entries.add(value);
        }

        return entries;
    }

    // ==================== Quicklist Parsers ====================

    private void parseListQuicklist(byte[] key) throws IOException {
        long count = readLength();
        List<byte[]> values = new ArrayList<>();
        for (long i = 0; i < count; i++) {
            byte[] ziplist = readString();
            values.addAll(parseZiplistEntries(ziplist));
        }
        visitor.onList(key, values, currentExpireTime);
    }

    private void parseListQuicklist2(byte[] key) throws IOException {
        long count = readLength();
        List<byte[]> values = new ArrayList<>();
        for (long i = 0; i < count; i++) {
            long container = readLength();
            byte[] data = readString();
            if (container == 1) {
                // Plain string node
                values.add(data);
            } else {
                // Listpack
                values.addAll(parseListpackEntries(data));
            }
        }
        visitor.onList(key, values, currentExpireTime);
    }

    // ==================== Listpack Parsers ====================

    private void parseHashListpack(byte[] key) throws IOException {
        byte[] listpack = readString();
        List<byte[]> entries = parseListpackEntries(listpack);

        Map<byte[], byte[]> fields = new LinkedHashMap<>();
        for (int i = 0; i < entries.size(); i += 2) {
            fields.put(entries.get(i), entries.get(i + 1));
        }
        visitor.onHash(key, fields, currentExpireTime);
    }

    private void parseSortedSetListpack(byte[] key) throws IOException {
        byte[] listpack = readString();
        List<byte[]> entries = parseListpackEntries(listpack);

        Map<byte[], Double> result = new LinkedHashMap<>();
        for (int i = 0; i < entries.size(); i += 2) {
            byte[] member = entries.get(i);
            byte[] scoreBytes = entries.get(i + 1);
            double score = Double.parseDouble(new String(scoreBytes, StandardCharsets.UTF_8));
            result.put(member, score);
        }
        visitor.onSortedSet(key, result, currentExpireTime);
    }

    private void parseSetListpack(byte[] key) throws IOException {
        byte[] listpack = readString();
        List<byte[]> members = parseListpackEntries(listpack);
        visitor.onSet(key, members, currentExpireTime);
    }

    private List<byte[]> parseListpackEntries(byte[] listpack) {
        // Listpack format:
        // <total-bytes><num-elements><element>...<element><end>

        int offset = 0;
        // long totalBytes = ByteUtils.readUint32LE(listpack, offset);
        offset += 4;
        int numElements = ByteUtils.readUint16LE(listpack, offset);
        offset += 2;

        List<byte[]> entries = new ArrayList<>(numElements);

        while (offset < listpack.length) {
            int b = listpack[offset] & 0xFF;
            if (b == 0xFF) {
                break; // End marker
            }

            byte[] value;

            if ((b & 0x80) == 0) {
                // 7-bit unsigned int
                value = String.valueOf(b).getBytes(StandardCharsets.UTF_8);
                offset++;
                offset++; // backlen
            } else if ((b & 0xC0) == 0x80) {
                // 6-bit string
                int len = b & 0x3F;
                offset++;
                value = Arrays.copyOfRange(listpack, offset, offset + len);
                offset += len;
                offset++; // backlen
            } else if ((b & 0xE0) == 0xC0) {
                // 13-bit signed int
                int val = ((b & 0x1F) << 8) | (listpack[offset + 1] & 0xFF);
                if ((val & 0x1000) != 0) {
                    val -= 0x2000; // Sign extend
                }
                value = String.valueOf(val).getBytes(StandardCharsets.UTF_8);
                offset += 2;
                offset++; // backlen
            } else if ((b & 0xF0) == 0xE0) {
                // 12-bit string
                int len = ((b & 0x0F) << 8) | (listpack[offset + 1] & 0xFF);
                offset += 2;
                value = Arrays.copyOfRange(listpack, offset, offset + len);
                offset += len;
                // Variable backlen
                offset += backlenSize(len + 2);
            } else if ((b & 0xFF) == 0xF0) {
                // 32-bit string
                offset++;
                int len = (int) ByteUtils.readUint32LE(listpack, offset);
                offset += 4;
                value = Arrays.copyOfRange(listpack, offset, offset + len);
                offset += len;
                offset += backlenSize(len + 5);
            } else if ((b & 0xFF) == 0xF1) {
                // 16-bit signed int
                offset++;
                int val = (short) ByteUtils.readUint16LE(listpack, offset);
                value = String.valueOf(val).getBytes(StandardCharsets.UTF_8);
                offset += 2;
                offset++; // backlen
            } else if ((b & 0xFF) == 0xF2) {
                // 24-bit signed int
                offset++;
                int val = (listpack[offset] & 0xFF) |
                          ((listpack[offset + 1] & 0xFF) << 8) |
                          ((listpack[offset + 2] & 0xFF) << 16);
                if ((val & 0x800000) != 0) {
                    val |= 0xFF000000;
                }
                value = String.valueOf(val).getBytes(StandardCharsets.UTF_8);
                offset += 3;
                offset++; // backlen
            } else if ((b & 0xFF) == 0xF3) {
                // 32-bit signed int
                offset++;
                int val = (int) ByteUtils.readUint32LE(listpack, offset);
                value = String.valueOf(val).getBytes(StandardCharsets.UTF_8);
                offset += 4;
                offset++; // backlen
            } else if ((b & 0xFF) == 0xF4) {
                // 64-bit signed int
                offset++;
                long val = ByteUtils.readInt64LE(listpack, offset);
                value = String.valueOf(val).getBytes(StandardCharsets.UTF_8);
                offset += 8;
                offset++; // backlen
            } else {
                throw new RdbParseException("Unknown listpack encoding: 0x" + Integer.toHexString(b));
            }

            entries.add(value);
        }

        return entries;
    }

    private int backlenSize(int len) {
        if (len < 128) return 1;
        if (len < 16384) return 2;
        if (len < 2097152) return 3;
        if (len < 268435456) return 4;
        return 5;
    }

    // ==================== Other Parsers ====================

    private void parseHashZipmap(byte[] key) throws IOException {
        byte[] zipmap = readString();
        Map<byte[], byte[]> fields = parseZipmap(zipmap);
        visitor.onHash(key, fields, currentExpireTime);
    }

    private Map<byte[], byte[]> parseZipmap(byte[] zipmap) {
        Map<byte[], byte[]> fields = new LinkedHashMap<>();
        int offset = 1; // Skip zmlen

        while (offset < zipmap.length) {
            int b = zipmap[offset] & 0xFF;
            if (b == 0xFF) {
                break;
            }

            // Read key
            int keyLen = readZipmapLength(zipmap, offset);
            offset += (keyLen < 254) ? 1 : 5;
            byte[] field = Arrays.copyOfRange(zipmap, offset, offset + keyLen);
            offset += keyLen;

            // Read value
            int valueLen = readZipmapLength(zipmap, offset);
            offset += (valueLen < 254) ? 1 : 5;
            int free = zipmap[offset++] & 0xFF;
            byte[] value = Arrays.copyOfRange(zipmap, offset, offset + valueLen);
            offset += valueLen + free;

            fields.put(field, value);
        }

        return fields;
    }

    private int readZipmapLength(byte[] data, int offset) {
        int b = data[offset] & 0xFF;
        if (b < 254) {
            return b;
        } else if (b == 254) {
            return (int) ByteUtils.readUint32LE(data, offset + 1);
        }
        return 0; // 255 is end marker
    }

    private void parseStream(byte[] key, int type) throws IOException {
        // Stream parsing is complex - for now, skip the data and notify
        // A full implementation would need to parse listpacks and stream metadata

        // Number of listpacks
        long listpacks = readLength();
        for (long i = 0; i < listpacks; i++) {
            readString(); // Stream ID as master entry key
            readString(); // Listpack
        }

        // Stream metadata
        readLength(); // length
        readLength(); // last_id ms
        readLength(); // last_id seq

        if (type >= RDB_TYPE_STREAM_LISTPACKS_2) {
            readLength(); // first_id ms
            readLength(); // first_id seq
            readLength(); // max_deleted_entry_id ms
            readLength(); // max_deleted_entry_id seq
            readLength(); // entries_added
        }

        if (type >= RDB_TYPE_STREAM_LISTPACKS_3) {
            // Additional fields in version 3
        }

        // Consumer groups
        long groups = readLength();
        for (long i = 0; i < groups; i++) {
            readString(); // Group name
            readLength(); // Last ID ms
            readLength(); // Last ID seq

            if (type >= RDB_TYPE_STREAM_LISTPACKS_2) {
                readLength(); // entries_read
            }

            // Pending entries (PEL)
            long pel = readLength();
            for (long j = 0; j < pel; j++) {
                in.readBytes(16); // Stream ID (16 bytes)
                in.readBytes(8);  // Delivery time
                readLength();     // Delivery count
            }

            // Consumers
            long consumers = readLength();
            for (long j = 0; j < consumers; j++) {
                readString();    // Consumer name
                in.readBytes(8); // Seen time

                if (type >= RDB_TYPE_STREAM_LISTPACKS_3) {
                    readLength(); // active time
                }

                // Consumer PEL (references global PEL)
                long consumerPel = readLength();
                for (long k = 0; k < consumerPel; k++) {
                    in.readBytes(16); // Stream ID
                }
            }
        }

        visitor.onStream(key, currentExpireTime);
    }

    private void parseModule(byte[] key, int type) throws IOException {
        long moduleId = readLength();
        String moduleName = moduleIdToName(moduleId);

        if (type == RDB_TYPE_MODULE_2) {
            // Module 2 has opcode-based serialization
            skipModule2Data();
        } else {
            // Module 1 - we can't know the size without the module
            throw new RdbParseException("Cannot parse MODULE type without the actual module");
        }

        visitor.onModule(key, moduleName, currentExpireTime);
    }

    private void skipModuleData() throws IOException {
        long moduleId = readLength();
        int when = (int) readLength();
        skipModule2Data();
    }

    private void skipModule2Data() throws IOException {
        // Module 2 uses opcodes until EOF opcode
        while (true) {
            long opcode = readLength();
            if (opcode == 0) { // EOF
                break;
            }
            switch ((int) opcode) {
                case 1 -> readLength(); // Signed int
                case 2 -> readLength(); // Unsigned int
                case 3 -> in.readDouble(); // Float
                case 4 -> in.readDouble(); // Double
                case 5 -> readString(); // String
                default -> throw new RdbParseException("Unknown module opcode: " + opcode);
            }
        }
    }

    private String moduleIdToName(long moduleId) {
        // Module ID is encoded as 64-bit with character set
        char[] charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_".toCharArray();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 9; i++) {
            int idx = (int) ((moduleId >> (i * 6)) & 0x3F);
            if (idx < charset.length) {
                sb.append(charset[idx]);
            }
        }
        return sb.reverse().toString().trim();
    }

    /**
     * Exception thrown when RDB parsing fails.
     */
    public static class RdbParseException extends RuntimeException {
        public RdbParseException(String message) {
            super(message);
        }

        public RdbParseException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
