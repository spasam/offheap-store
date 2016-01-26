package com.onshape.cache;

import java.nio.ByteBuffer;
import java.util.List;

import com.onshape.cache.exception.CacheException;

/**
 * Internal cache interface that provides CRUD and internal management opertations.
 *
 * @author Seshu Pasam
 */
public interface Cache {
    /**
     * Put the specified key/value with the provided expiration in cache.
     *
     * @param key Cache key.
     * @param value Value as bytes.
     * @param expireSecs Expiration in seconds from now. {@code 0} if the entry should never expire.
     * @param useOffHeap Hint on whether to use OffHeap or not. This does not guarantee that the entry will be cached in
     *            OffHeap.
     */
    void put(String key, byte[] value, int expireSecs, boolean useOffHeap) throws CacheException;

    /**
     * Returns the cached data for the specified key.
     *
     * @param key Cache key.
     * @return Cache data as byte buffer. {@code null} is returned if the key is not found in cache.
     */
    ByteBuffer get(String key) throws CacheException;

    /**
     * Checks to see if the specified key exists in cache.
     *
     * @param key Cache key.
     * @return {@code true}, if the entry is found in cache. {@code false} otherwise.
     */
    boolean contains(String key) throws CacheException;

    /**
     * Removes the specified key from the cache. If the entry is not found in cache, this method returns silently.
     *
     * @param key Cache key.
     */
    void remove(String key) throws CacheException;

    /**
     * Removes all keys matching the specified cache prefix.
     *
     * @param prefix Cache key prefix.
     */
    void removeHierarchy(String prefix) throws CacheException;

    /**
     * Method to force cleanup of expired entries. Cache periodically cleans up expired entries.
     */
    boolean cleanupExpired();

    /**
     * Returns list of cache keys that match the specified prefix.
     *
     * @param prefix Cache prefix.
     * @return List of cache keys that match the prefix. Or empty list, if none found.
     */
    List<String> list(String prefix) throws CacheException;
}
