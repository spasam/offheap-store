package com.onshape.cache.impl;

import java.nio.ByteBuffer;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
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
        long start = System.currentTimeMillis();
        diskStore.getKeys((String key, Integer expiresAtSecs) -> onHeap.put(key, expiresAtSecs));
        LOG.info("Keys from disk loaded in: {} ms", (System.currentTimeMillis() - start));
    }

    @Override
    public void put(String key, byte[] value, int expireSecs, boolean useOffHeap) throws CacheException {
        int expiresAtSecs = 0;
        if (expireSecs > 0) {
            expiresAtSecs = (int) (System.currentTimeMillis() / 1000L) + expireSecs;
        }

        onHeap.put(key, expiresAtSecs);
        if (useOffHeap && offHeap.accepts(value.length)) {
            offHeap.putAsync(key, value);
        }
        diskStore.putAsync(key, value, expiresAtSecs);
    }

    @Override
    public ByteBuffer get(String key) throws CacheException {
        if (!onHeap.contains(key)) {
            return null;
        }

        ByteBuffer buffer = offHeap.get(key);
        if (buffer == null) {
            buffer = diskStore.get(key);
        }

        return buffer;
    }

    @Override
    public List<String> list(String prefix) throws CacheException {
        return diskStore.list(prefix);
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
        return onHeap.contains(key);
    }

    @Override
    public void removeHierarchy(String prefix) throws CacheException {
        diskStore.checkHierarchy(prefix);
        diskStore.removeHierarchyAsync(prefix, (String key) -> {
            if (key != null) {
                LOG.info("Delete entry (hierarchy): {}", key);
                onHeap.remove(key);
                if (offHeap.isEnabled()) {
                    offHeap.removeAsync(key);
                }
            }
        });
    }

    @Override
    @Scheduled(initialDelay = 3600_000L, fixedDelayString = "${expiredCleanupDelayMs}")
    public void cleanupExpired() {
        long start = System.currentTimeMillis();
        LOG.info("Running expired cleanup task");
        onHeap.cleanupExpired((String key) -> {
            try {
                if (offHeap.isEnabled()) {
                    offHeap.removeAsync(key);
                }
                diskStore.removeAsync(key);
            }
            catch (Exception e) {
                LOG.error("Error deleting expired entry: {}", key);
            }
        });
        LOG.info("Expired cleanup task completed in: {} ms", (System.currentTimeMillis() - start));
    }
}
