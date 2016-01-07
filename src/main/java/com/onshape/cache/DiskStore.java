package com.onshape.cache;

import java.nio.ByteBuffer;
import java.util.function.Function;

import com.onshape.cache.exception.CacheException;

public interface DiskStore {
    void putAsync(String key, byte[] value) throws CacheException;

    ByteBuffer get(String key) throws CacheException;

    boolean contains(String key) throws CacheException;

    void removeAsync(String key) throws CacheException;

    void removeHierarchy(String prefix, Function<String, Void> function) throws CacheException;
}
