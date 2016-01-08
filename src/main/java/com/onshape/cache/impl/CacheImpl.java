package com.onshape.cache.impl;

import java.nio.ByteBuffer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.onshape.cache.Cache;
import com.onshape.cache.DiskStore;
import com.onshape.cache.OffHeap;
import com.onshape.cache.OnHeap;
import com.onshape.cache.exception.CacheException;

@Service
public class CacheImpl implements Cache, InitializingBean {
    private static final Logger LOG = LoggerFactory.getLogger(CacheImpl.class);

    @Autowired
    private OnHeap onHeap;
    @Autowired
    private OffHeap offHeap;
    @Autowired
    private DiskStore diskStore;

    @Override
    public void afterPropertiesSet() throws Exception {
        diskStore.startScavengerAsync(new Function<String, Void>() {
            @Override
            public Void apply(String key) {
                if (key != null) {
                    LOG.info("Delete expired entry: {}", key);
                    onHeap.remove(key);
                    offHeap.removeAsync(key);
                }
                return null;
            }
        });
    }

    @Override
    public void put(String key, byte[] value, int expireSecs) throws CacheException {
        onHeap.put(key);
        if (offHeap.accepts(value.length)) {
            offHeap.putAsync(key, value);
        }
        diskStore.putAsync(key, value, expireSecs);
    }

    @Override
    public ByteBuffer get(String key) throws CacheException {
        // ByteBuffer is thread local. So all of these calls have to be synchronous
        ByteBuffer buffer = offHeap.get(key);
        if (buffer == null) {
            buffer = diskStore.get(key);
            if (buffer != null) {
                offHeap.put(key, buffer);
            }
        }

        if (buffer != null && !onHeap.contains(key)) {
            onHeap.put(key);
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

    @Override
    public void removeHierarchy(String prefix) throws CacheException {
        diskStore.checkHierarchy(prefix);
        diskStore.removeHierarchyAsync(prefix, new Function<String, Void>() {
            @Override
            public Void apply(String key) {
                if (key != null) {
                    LOG.info("Delete entry: {}", key);
                    onHeap.remove(key);
                    offHeap.removeAsync(key);
                }
                return null;
            }
        });
    }
}
