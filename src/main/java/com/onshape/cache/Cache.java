package com.onshape.cache;

import java.nio.ByteBuffer;

import com.onshape.cache.exception.CacheException;

public interface Cache {
    void put(String key, byte[] value, int expireSecs) throws CacheException;

    ByteBuffer get(String key) throws CacheException;

    boolean contains(String key) throws CacheException;

    void remove(String key) throws CacheException;

    void removeHierarchy(String prefix) throws CacheException;

    void cleanupExpired();
}
