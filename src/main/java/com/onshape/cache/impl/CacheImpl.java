package com.onshape.cache.impl;

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
    public byte[] get(String key) throws CacheException {
        byte[] value = onHeap.get(key);
        if (value != null) {
            return value;
        }

        value = diskStore.get(key);
        if (value != null) {
            onHeap.put(key, value);
        }

        return value;
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
