package com.onshape.cache.impl;

import java.nio.ByteBuffer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.onshape.cache.Cache;
import com.onshape.cache.DiskStore;
import com.onshape.cache.OffHeap;
import com.onshape.cache.OnHeap;
import com.onshape.cache.exception.CacheException;

@Service("cache")
public class CacheImpl implements Cache {
    @Autowired
    private OnHeap onHeap;
    @Autowired
    private OffHeap offHeap;
    @Autowired
    private DiskStore diskStore;

    @Override
    public void put(String key, byte[] value) throws CacheException {
        onHeap.put(key);
        if (offHeap.accepts(value.length)) {
            offHeap.putAsync(key, value, value.length);
        }
        diskStore.putAsync(key, value);
    }

    @Override
    public ByteBuffer get(String key) throws CacheException {
        // ByteBuffer is thread local. So all of these calls have to be synchronous
        ByteBuffer buffer = offHeap.get(key);
        if (buffer != null) {
            return buffer;
        }

        buffer = diskStore.get(key);
        if (buffer != null) {
            offHeap.put(key, buffer.array(), buffer.position());
        }

        return buffer;
    }

    @Override
    public void remove(String key) throws CacheException {
        if (onHeap.remove(key)) {
            if (offHeap.isEnabled()) {
                offHeap.removeAsync(key);
            }
            diskStore.removeAsync(key);
        }
    }

    @Override
    public boolean contains(String key) throws CacheException {
        if (!onHeap.contains(key)) {
            // If this service was restarted, onheap entries will be empty. Re-populate
            if (diskStore.contains(key)) {
                onHeap.put(key);
                return true;
            }

            return false;
        }

        return true;
    }
}
