package com.onshape.cache.impl;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import com.onshape.cache.Cache;
import com.onshape.cache.DiskStore;
import com.onshape.cache.OffHeap;
import com.onshape.cache.OnHeap;
import com.onshape.cache.exception.CacheException;

/**
 * Cache implementation.
 *
 * @author Seshu Pasam
 */
@Service
public class CacheImpl implements Cache, InitializingBean {
    private static final Logger LOG = LoggerFactory.getLogger(CacheImpl.class);

    @Autowired
    private OnHeap onHeap;
    @Autowired
    private OffHeap offHeap;
    @Autowired
    private DiskStore diskStore;
    @Autowired
    private ThreadPoolTaskExecutor executor;

    /** Lock help when cleaning up expired entries */
    private Lock cleanupLock;

    @Override
    public void afterPropertiesSet() throws Exception {
        cleanupLock = new ReentrantLock();

        // Load existing key/expiration information from disk
        Map<String, Integer> existingKeys = diskStore.readKeys();
        if (existingKeys != null) {
            LOG.info("Loaded existing keys/expiration information. Size: {}", existingKeys.size());
        }

        // Initialize on heap storage
        onHeap.init(existingKeys);

        if (existingKeys == null) {
            // If existing keys information is not found or corrupt, do the expensive loading
            long start = System.currentTimeMillis();
            diskStore.getKeys((String key, Integer expiresAtSecs) -> onHeap.put(key, expiresAtSecs));
            LOG.info("Keys from disk loaded in: {} ms", (System.currentTimeMillis() - start));
        }
    }

    @Override
    public void put(String key, byte[] value, int expireSecs, boolean useOffHeap) throws CacheException {
        final int expiresAtSecs = (expireSecs > 0)
                        ? (int) (System.currentTimeMillis() / 1000L) + expireSecs
                        : 0;

        // Synchronously put it off heap or on disk. If the put in off heap succeeds, put on disk asynchronously
        boolean putInOffHeap = false;
        if (useOffHeap && offHeap.accepts(value.length)) {
            putInOffHeap = offHeap.put(key, value);
        }

        if (putInOffHeap) {
            onHeap.put(key, expiresAtSecs);
            diskStore.putAsync(key, value, expiresAtSecs,
                            (String failedKey) -> {
                                onHeap.remove(failedKey);
                                offHeap.removeAsync(failedKey);
                                return null;
                            });
        } else {
            diskStore.put(key, value, expiresAtSecs,
                            (String successKey) -> {
                                onHeap.put(successKey, expiresAtSecs);
                                return null;
                            });
        }
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
        onHeap.remove(key);
        if (offHeap.isEnabled()) {
            offHeap.removeAsync(key);
        }
        diskStore.removeAsync(key);
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
        if (cleanupLock.tryLock()) {
            try {
                long start = System.currentTimeMillis();
                LOG.info("Running expired cleanup task");
                onHeap.cleanupExpired((String key) -> {
                    try {
                        if (offHeap.isEnabled()) {
                            offHeap.removeAsync(key);
                        }
                        diskStore.removeAsync(key);
                    } catch (Exception e) {
                        LOG.error("Error deleting expired entry: {}", key);
                    }
                });
                LOG.info("Expired cleanup task completed in: {} ms", (System.currentTimeMillis() - start));
            } finally {
                cleanupLock.unlock();
            }
        } else {
            LOG.warn("Cleanup is already running. Ignoring request");
        }
    }

    @Override
    public void shutdown() throws CacheException {
        // Acquire cleanup lock so that cleanup will not run. Do not release it because we are shutting down
        LOG.debug("Waiting for cleanup lock");
        cleanupLock.lock();

        // Wait for executor to shutdown
        LOG.debug("Shutting down executor");
        executor.shutdown();

        try {
            // Wait for any other pending tasks
            Thread.sleep(5000L);

            // Flush the key map to disk
            LOG.debug("Flushing keys to disk");
            diskStore.writeKeys(onHeap.getKeys());
        } catch (Exception e) {
            throw new CacheException("Error shutting down cache server", e);
        }

        System.exit(0);
    }
}
