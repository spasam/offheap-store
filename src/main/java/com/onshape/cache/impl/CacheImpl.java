package com.onshape.cache.impl;

import java.nio.ByteBuffer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.onshape.cache.Cache;
import com.onshape.cache.DiskStore;
import com.onshape.cache.OnHeap;
import com.onshape.cache.exception.CacheException;

@Service("cache")
public class CacheImpl implements Cache {
    @Autowired
    private OnHeap onHeap;
    @Autowired
    private DiskStore diskStore;

    @Override
    public void put(String key, byte[] value) throws CacheException {
        onHeap.put(key, value);
        diskStore.put(key, value);
    }

    @Override
    public ByteBuffer get(String key) throws CacheException {
        ByteBuffer buffer = onHeap.get(key);
        if (buffer != null) {
            return buffer;
        }

        buffer = diskStore.get(key);
        if (buffer != null) {
            onHeap.put(key, buffer.array(), buffer.position());
        }

        return buffer;
    }

    @Override
    public void remove(String key) throws CacheException {
        onHeap.remove(key);
        diskStore.remove(key);
    }

    @Override
    public boolean contains(String key) throws CacheException {
        if (!onHeap.contains(key)) {
            return diskStore.contains(key);
        }

        return true;
    }
}
