package com.onshape.cache;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.onshape.cache.exception.CacheException;

public interface DiskStore {
    void putAsync(String key, byte[] value, int expireSecs) throws CacheException;

    ByteBuffer get(String key) throws CacheException;

    void removeAsync(String key) throws CacheException;

    void checkHierarchy(String prefix) throws CacheException;

    void removeHierarchyAsync(String prefix, Consumer<String> consumer) throws CacheException;

    void getKeys(BiConsumer<String, Integer> consumer) throws InterruptedException, ExecutionException;
}
