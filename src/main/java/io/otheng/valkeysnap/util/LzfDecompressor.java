package io.otheng.valkeysnap.util;

/**
 * LZF decompression implementation for RDB compressed strings.
 * LZF is a fast compression algorithm used by Redis for string compression.
 */
public final class LzfDecompressor {

    private LzfDecompressor() {}

    /**
     * Decompresses LZF-compressed data.
     *
     * @param compressed the compressed data
     * @param uncompressedLength the expected uncompressed length
     * @return the decompressed data
     * @throws IllegalArgumentException if decompression fails
     */
    public static byte[] decompress(byte[] compressed, int uncompressedLength) {
        byte[] output = new byte[uncompressedLength];
        int inPos = 0;
        int outPos = 0;

        while (inPos < compressed.length && outPos < uncompressedLength) {
            int ctrl = compressed[inPos++] & 0xFF;

            if (ctrl < 32) {
                // Literal run: copy ctrl+1 bytes
                int len = ctrl + 1;
                if (inPos + len > compressed.length || outPos + len > uncompressedLength) {
                    throw new IllegalArgumentException("LZF decompression failed: literal overflow");
                }
                System.arraycopy(compressed, inPos, output, outPos, len);
                inPos += len;
                outPos += len;
            } else {
                // Back-reference
                int len = (ctrl >> 5) + 2;
                int offset;

                if (len == 9) {
                    // Long match: length is in next byte + 9
                    if (inPos >= compressed.length) {
                        throw new IllegalArgumentException("LZF decompression failed: unexpected end");
                    }
                    len = (compressed[inPos++] & 0xFF) + 9;
                }

                if (inPos >= compressed.length) {
                    throw new IllegalArgumentException("LZF decompression failed: unexpected end");
                }

                offset = ((ctrl & 0x1F) << 8) | (compressed[inPos++] & 0xFF);
                offset += 1; // Offset is stored as offset-1

                if (outPos - offset < 0 || outPos + len > uncompressedLength) {
                    throw new IllegalArgumentException("LZF decompression failed: back-reference out of bounds");
                }

                // Copy from back-reference (may overlap)
                int srcPos = outPos - offset;
                for (int i = 0; i < len; i++) {
                    output[outPos++] = output[srcPos++];
                }
            }
        }

        if (outPos != uncompressedLength) {
            throw new IllegalArgumentException(
                "LZF decompression failed: expected " + uncompressedLength + " bytes, got " + outPos
            );
        }

        return output;
    }
}
