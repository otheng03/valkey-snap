package io.otheng.valkeysnap.network;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * An InputStream wrapper that implements token bucket rate limiting.
 * Limits the rate at which bytes can be read from the underlying stream.
 */
public class RateLimitInputStream extends FilterInputStream {

    private final long bytesPerSecond;
    private final long tokenRefillIntervalNanos;
    private long availableTokens;
    private long lastRefillTime;
    private final long maxTokens;

    /**
     * Creates a rate-limited input stream.
     *
     * @param in the underlying input stream
     * @param bytesPerSecond maximum bytes per second (0 = unlimited)
     */
    public RateLimitInputStream(InputStream in, long bytesPerSecond) {
        super(in);
        this.bytesPerSecond = bytesPerSecond;

        if (bytesPerSecond > 0) {
            // Allow burst up to 1 second worth of data
            this.maxTokens = bytesPerSecond;
            this.availableTokens = maxTokens;
            this.tokenRefillIntervalNanos = 1_000_000_000L; // 1 second in nanos
            this.lastRefillTime = System.nanoTime();
        } else {
            // No rate limiting
            this.maxTokens = Long.MAX_VALUE;
            this.availableTokens = Long.MAX_VALUE;
            this.tokenRefillIntervalNanos = 0;
            this.lastRefillTime = 0;
        }
    }

    @Override
    public int read() throws IOException {
        if (bytesPerSecond > 0) {
            acquireTokens(1);
        }
        return super.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (bytesPerSecond <= 0) {
            return super.read(b, off, len);
        }

        // Limit read size to available tokens
        int toRead = (int) Math.min(len, acquireTokens(len));
        return super.read(b, off, toRead);
    }

    /**
     * Acquires tokens for reading, blocking if necessary.
     * Returns the number of tokens actually acquired.
     */
    private synchronized long acquireTokens(int requested) {
        refillTokens();

        while (availableTokens <= 0) {
            // Calculate how long to wait for at least one token
            long tokensNeeded = Math.min(requested, maxTokens);
            long nanosToWait = (tokensNeeded * tokenRefillIntervalNanos) / bytesPerSecond;

            try {
                long millisToWait = nanosToWait / 1_000_000;
                int nanosRemainder = (int) (nanosToWait % 1_000_000);
                if (millisToWait > 0 || nanosRemainder > 0) {
                    Thread.sleep(millisToWait, nanosRemainder);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for rate limit tokens", e);
            }

            refillTokens();
        }

        long acquired = Math.min(requested, availableTokens);
        availableTokens -= acquired;
        return acquired;
    }

    /**
     * Refills tokens based on elapsed time since last refill.
     */
    private void refillTokens() {
        if (bytesPerSecond <= 0) {
            return;
        }

        long now = System.nanoTime();
        long elapsedNanos = now - lastRefillTime;

        if (elapsedNanos > 0) {
            long tokensToAdd = (elapsedNanos * bytesPerSecond) / tokenRefillIntervalNanos;
            if (tokensToAdd > 0) {
                availableTokens = Math.min(maxTokens, availableTokens + tokensToAdd);
                lastRefillTime = now;
            }
        }
    }

    /**
     * Returns the configured rate limit in bytes per second.
     */
    public long getBytesPerSecond() {
        return bytesPerSecond;
    }

    /**
     * Returns whether rate limiting is enabled.
     */
    public boolean isRateLimitEnabled() {
        return bytesPerSecond > 0;
    }
}
