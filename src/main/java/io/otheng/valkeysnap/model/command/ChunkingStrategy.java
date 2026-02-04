package io.otheng.valkeysnap.model.command;

/**
 * Configuration for how large collections should be chunked into multiple commands.
 *
 * @param maxElements maximum number of elements per command (e.g., 1000 members per SADD)
 * @param maxBytes maximum bytes per command (soft limit based on argument sizes)
 */
public record ChunkingStrategy(int maxElements, int maxBytes) {

    /**
     * Default chunking: 1000 elements, 64KB max bytes.
     */
    public static final ChunkingStrategy DEFAULT = new ChunkingStrategy(1000, 64 * 1024);

    /**
     * No chunking - emit single commands regardless of size.
     */
    public static final ChunkingStrategy NONE = new ChunkingStrategy(Integer.MAX_VALUE, Integer.MAX_VALUE);

    /**
     * Conservative chunking for networks with smaller buffers.
     */
    public static final ChunkingStrategy CONSERVATIVE = new ChunkingStrategy(100, 16 * 1024);

    public ChunkingStrategy {
        if (maxElements < 1) {
            throw new IllegalArgumentException("maxElements must be at least 1");
        }
        if (maxBytes < 1) {
            throw new IllegalArgumentException("maxBytes must be at least 1");
        }
    }

    /**
     * Creates a strategy with the specified max elements.
     */
    public static ChunkingStrategy byElements(int maxElements) {
        return new ChunkingStrategy(maxElements, Integer.MAX_VALUE);
    }

    /**
     * Creates a strategy with the specified max bytes.
     */
    public static ChunkingStrategy byBytes(int maxBytes) {
        return new ChunkingStrategy(Integer.MAX_VALUE, maxBytes);
    }

    /**
     * Calculates the number of chunks needed for a collection.
     *
     * @param elementCount number of elements
     * @param totalBytes total bytes of all elements
     * @return estimated number of chunks
     */
    public int estimateChunks(int elementCount, long totalBytes) {
        int byElementChunks = (elementCount + maxElements - 1) / maxElements;
        int byByteChunks = (int) ((totalBytes + maxBytes - 1) / maxBytes);
        return Math.max(byElementChunks, byByteChunks);
    }
}
