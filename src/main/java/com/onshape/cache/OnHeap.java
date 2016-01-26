package com.onshape.cache;

import java.util.function.Consumer;

/**
 * CRUD operations for on heap store. On heap store does not cache data, only stores cache keys and expiration
 * information.
 *
 * @author Seshu Pasam
 */
public interface OnHeap {
    /**
     * Save the specified cache key with provided expiration information.
     *
     * @param key Cache key.
     * @param expiresAtSecs When the cache entry expires (in seconds from epoch).
     */
    void put(String key, int expiresAtSecs);

    /**
     * Check if the specified key exists in cache.
     *
     * @param key Cache key.
     * @return {@code true}, if the key exists. {@code false} otherwise.
     */
    boolean contains(String key);

    /**
     * Remove the specified key from cache. If the key is not found, this method just returns.
     *
     * @param key Cache key.
     * @return {@code true}, if the cache key was found and removed. {@code false} otherwise.
     */
    boolean remove(String key);

    /**
     * Cleanup expired cache entries.
     *
     * @param consumer Consumer that should be invoked with each expired cache key.
     */
    void cleanupExpired(Consumer<String> consumer);
}
