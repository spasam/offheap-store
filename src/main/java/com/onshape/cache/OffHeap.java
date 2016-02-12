package com.onshape.cache;

import java.nio.ByteBuffer;

/**
 * CRUD operations for off heap store.
 *
 * @author Seshu Pasam
 */
public interface OffHeap {
    /**
     * Put cache key/data in off heap store.
     *
     * @param key Cache key.
     * @param value Cache data.
     * @return {@code true} if successfully put in off heap. {@code false} otherwise.
     */
    boolean put(String key, byte[] value);

    /**
     * Returns data associated with specified cache key. {@code null} is returned if the cache key is not found.
     *
     * @param key Cache key.
     * @return Cache data as byte buffer.
     */
    ByteBuffer get(String key);

    /**
     * Asynchronously removes specified cache key from off heap. If the key is not found, this method just returns.
     *
     * @param key Cache key.
     */
    void removeAsync(String key);

    /**
     * Whether off heap store is enabled or not.
     *
     * @return {@code true} if off heap store is enabled. {@code false} otherwise.
     */
    boolean isEnabled();

    /**
     * Returns {@code true} if off heap store is enabled and is capable of caching data with specified length.
     *
     * @param length Cache data length.
     * @return {@code true} if acceptable. {@code false} otherwise.
     */
    boolean accepts(int length);
}
