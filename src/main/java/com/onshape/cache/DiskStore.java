package com.onshape.cache;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import com.onshape.cache.exception.CacheException;

/**
 * CRUD operations for disk persisted cache.
 *
 * @author Seshu Pasam
 */
public interface DiskStore {
    /**
     * Asynchronously save the specified cache key/value data on disk. If an entry already exists on disk, it will be
     * overwritten. Expiration information will be persisted as extended user attributes on the file.
     *
     * @param key Cache key (file path)
     * @param value Cache data (file contents)
     * @param expiresAtSecs When the cache entry expires (in seconds from epoch).
     * @param onError Function to be applied if there is a problem writing the data to the disk.
     */
    void putAsync(String key, byte[] value, int expiresAtSecs, Function<String, Void> onError) throws CacheException;

    /**
     * Synchronously save the specified cache key/value data on disk. If an entry already exists on disk, it will be
     * overwritten. Expiration information will be persisted as extended user attributes on the file.
     *
     * @param key Cache key (file path)
     * @param value Cache data (file contents)
     * @param expiresAtSecs When the cache entry expires (in seconds from epoch).
     * @param onSuccess Function to be applied if the data to the disk is successfully written to disk.
     */
    void put(String key, byte[] value, int expiresAtSecs, Function<String, Void> onSuccess) throws CacheException;

    /**
     * Returns data for the specified cache key as byte buffer.
     *
     * @param key Cache key.
     * @return Cache data or {@code null} if the entry is not found.
     */
    ByteBuffer get(String key) throws CacheException;

    /**
     * Asynchronously removes the specified cache key from disk. If the entry is not found, this method just returns.
     *
     * @param key Cache key.
     */
    void removeAsync(String key) throws CacheException;

    /**
     * Checks to see if there is a cache hierarchy with the specified prefix. If none of the entries match the specified
     * prefix, {@code EntryNotFoundException} is thrown.
     *
     * @param prefix Cache prefix.
     */
    void checkHierarchy(String prefix) throws CacheException;

    /**
     * Asynchronously removes cache entries matching the specified prefix.
     *
     * @param prefix Cache prefix.
     * @param consumer Consumer to be invoked with each cache key matching the prefix.
     */
    void removeHierarchyAsync(String prefix, Consumer<String> consumer) throws CacheException;

    /**
     * Method to load cache keys and expiration information from disk.
     *
     * @param consumer Consumer to be called with each cache key and its expiration.
     */
    void getKeys(BiConsumer<String, Integer> consumer) throws InterruptedException, ExecutionException;

    /**
     * Returns list of cache keys matching the specified prefix. If none are found empty list is returned.
     *
     * @param prefix Cache prefix.
     * @return Cache keys matching the specified prefix.
     */
    List<String> list(String prefix) throws CacheException;
}
