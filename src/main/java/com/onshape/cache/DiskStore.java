package com.onshape.cache;

import java.nio.ByteBuffer;
import java.util.function.Function;

import com.onshape.cache.exception.CacheException;

public interface DiskStore {
    void putAsync(String key, byte[] value, int expireSecs) throws CacheException;

    ByteBuffer get(String key) throws CacheException;

    boolean contains(String key) throws CacheException;

    void removeAsync(String key) throws CacheException;

    void checkHierarchy(String prefix) throws CacheException;

    void removeHierarchyAsync(String prefix, Function<String, Void> deleteFunction) throws CacheException;

    void startScavengerAsync(Function<String, Void> deleteFunction);

    void pokeScavenger();
}
