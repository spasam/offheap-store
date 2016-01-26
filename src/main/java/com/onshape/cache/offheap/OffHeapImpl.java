package com.onshape.cache.offheap;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalNotification;
import com.onshape.cache.OffHeap;
import com.onshape.cache.buffer.BufferPool;
import com.onshape.cache.buffer.CompositeByteBuffer;
import com.onshape.cache.metrics.MetricService;

/**
 * Off heap store implementation.
 *
 * @author Seshu Pasam
 */
@Service
public class OffHeapImpl implements OffHeap, InitializingBean, HealthIndicator {
    private static final Logger LOG = LoggerFactory.getLogger(OffHeapImpl.class);

    /** Format of off heap entry */
    private static class HeapEntry {
        private final int sizeBytes;
        private final int normalizedSizeBytes;
        private final CompositeByteBuffer buffer;

        private HeapEntry(int sizeBytes, int normalizedSizeBytes, CompositeByteBuffer buffer) {
            this.sizeBytes = sizeBytes;
            this.normalizedSizeBytes = normalizedSizeBytes;
            this.buffer = buffer;
        }
    }

    /** Thread local cache for temporary direct byte buffers */
    private class ByteBufferCache extends ThreadLocal<ByteBuffer> {
        @Override
        protected ByteBuffer initialValue() {
            ms.increment("byte.buffer.direct.count");
            ms.increment("byte.buffer.direct.size", maxOffHeapEntrySizeBytes);
            return ByteBuffer.allocateDirect(maxOffHeapEntrySizeBytes);
        }

        public ByteBuffer getOne() {
            ByteBuffer buffer = get();
            buffer.clear();

            return buffer;
        }
    }

    @Autowired
    private MetricService ms;
    @Autowired
    private BufferPool pool;

    @Value("${offHeapDisabled}")
    private boolean offHeapDisabled;
    @Value("${maxOffHeapSizeBytes}")
    private long maxOffHeapSizeBytes;
    @Value("${maxOffHeapEntrySizeBytes}")
    private int maxOffHeapEntrySizeBytes;
    @Value("${offHeapChunkSizeBytes}")
    private int offHeapChunkSizeBytes;
    @Value("${server.tomcat.max-threads}")
    private int concurrencyLevel;
    @Value("${evictionThreshold}")
    private int evictionThreshold;
    @Value("${blockDurationMs}")
    private int blockDurationMs;

    /** Whether to skip off heap store temporarily */
    private AtomicBoolean temporarySkipOffHeap;
    /** Amount of off heap bytes used */
    private AtomicLong allocatedOffHeapSize;

    /** Read/write locks for concurrent access to off heap entries */
    private Lock[] readLocks;
    private Lock[] writeLocks;

    /** Thread local cache for direct byte buffers */
    private ByteBufferCache byteBufferCache;

    /** Cache of off heap entries */
    private Cache<String, HeapEntry> offHeapEntries;

    /** List of entries evicted from cache that should be lazily cleaned up */
    private List<RemovalNotification<String, HeapEntry>> lazyCleaningList = Collections.synchronizedList(
                    new LinkedList<>());

    @Override
    public void afterPropertiesSet() throws Exception {
        if (offHeapDisabled) {
            return;
        }

        long usableOffHeapSizeBytes = maxOffHeapSizeBytes - (concurrencyLevel * maxOffHeapEntrySizeBytes);
        usableOffHeapSizeBytes = Math.round(usableOffHeapSizeBytes * 0.90d);
        int maxOffHeapEntries = (int) (usableOffHeapSizeBytes / offHeapChunkSizeBytes);

        LOG.info("Max offheap size bytes: {}", maxOffHeapSizeBytes);
        LOG.info("Usable offheap size bytes: {}", usableOffHeapSizeBytes);
        LOG.info("Max offheap entry size bytes: {}", maxOffHeapEntrySizeBytes);
        LOG.info("Max offheap entries: {}", maxOffHeapEntries);
        LOG.info("Concurrent level: {}", concurrencyLevel);
        LOG.info("Eviction threshold: {}", evictionThreshold);
        LOG.info("Blocking duration: {} ms", blockDurationMs);

        temporarySkipOffHeap = new AtomicBoolean(false);
        allocatedOffHeapSize = new AtomicLong(0);

        readLocks = new Lock[concurrencyLevel];
        writeLocks = new Lock[concurrencyLevel];
        for (int i = 0; i < concurrencyLevel; i++) {
            ReadWriteLock rwLock = new ReentrantReadWriteLock();
            readLocks[i] = rwLock.readLock();
            writeLocks[i] = rwLock.writeLock();
        }

        byteBufferCache = new ByteBufferCache();

        offHeapEntries = CacheBuilder.newBuilder()
                        .initialCapacity(maxOffHeapEntries)
                        .maximumWeight(maxOffHeapSizeBytes)
                        .weigher((String key, HeapEntry value) -> value.normalizedSizeBytes)
                        .removalListener((RemovalNotification<String, HeapEntry> rn) -> lazyCleaningList.add(rn))
                        .build();

        Executors.newSingleThreadExecutor((Runnable r) -> new Thread(r, "oh-cleaner"))
                        .submit(() -> freeOffHeapEntries());
    }

    @Async
    @Override
    public void putAsync(String key, byte[] value) {
        long start = System.currentTimeMillis();
        int length = value.length;
        int normalizedSizeBytes = (int) Math.ceil((double) length / offHeapChunkSizeBytes) * offHeapChunkSizeBytes;

        CompositeByteBuffer buf = pool.get((int) Math.ceil((double) normalizedSizeBytes / offHeapChunkSizeBytes));
        if (buf == null) {
            ms.increment("offheap.allocation.failure");
            return;
        }

        buf.writeBytes(value);

        // If we are replacing the value, removal notification will be called with old value
        // Removal notification will take care of cleaning old heap entry
        offHeapEntries.put(key, new HeapEntry(length, normalizedSizeBytes, buf));

        allocatedOffHeapSize.addAndGet(normalizedSizeBytes);
        ms.increment("offheap.size", normalizedSizeBytes);
        ms.increment("offheap.wasted", (normalizedSizeBytes - length));
        ms.increment("offheap.count");
        ms.reportMetrics("offheap.put", start);
    }

    @Override
    public ByteBuffer get(String key) {
        if (offHeapDisabled) {
            return null;
        }

        long start = System.currentTimeMillis();
        ByteBuffer buffer = byteBufferCache.getOne();

        Lock readLock = readLocks[Math.abs(key.hashCode()) % concurrencyLevel];
        readLock.lock();
        try {
            HeapEntry heapEntry = offHeapEntries.getIfPresent(key);
            if (heapEntry == null) {
                ms.increment("offheap.get.miss");
                return null;
            }

            buffer.limit(heapEntry.sizeBytes);
            heapEntry.buffer.getBytes(buffer);
        } finally {
            readLock.unlock();
        }

        buffer.flip();
        ms.reportMetrics("offheap.get", start);

        return buffer;
    }

    @Async
    @Override
    public void removeAsync(String key) {
        if (offHeapDisabled) {
            return;
        }

        long start = System.currentTimeMillis();

        Lock writeLock = writeLocks[Math.abs(key.hashCode()) % concurrencyLevel];
        writeLock.lock();
        try {
            offHeapEntries.invalidate(key);
        } finally {
            writeLock.unlock();
        }

        ms.reportMetrics("offheap.delete", start);
    }

    @Override
    public boolean isEnabled() {
        return !offHeapDisabled;
    }

    @Override
    public boolean accepts(int sizeBytes) {
        return !offHeapDisabled && sizeBytes <= maxOffHeapEntrySizeBytes && !temporarySkipOffHeap.get();
    }

    @Override
    public Health health() {
        NumberFormat formatter = new DecimalFormat("#0.00");
        return new Health.Builder().up()
                        .withDetail("% full", formatter
                                        .format(((double) allocatedOffHeapSize.get() / maxOffHeapSizeBytes) * 100))
                        .build();
    }

    private void freeOffHeapEntries() {
        long blockedAt = 0;

        while (true) {
            int size = lazyCleaningList.size();

            while (!lazyCleaningList.isEmpty()) {
                RemovalNotification<String, HeapEntry> notification = lazyCleaningList.remove(0);
                RemovalCause cause = notification.getCause();
                Assert.isTrue(cause == RemovalCause.SIZE // Size exceeded
                                || cause == RemovalCause.EXPLICIT // Manually deleted by user
                                || cause == RemovalCause.REPLACED); // Entry is being replaced

                HeapEntry heapEntry;
                Lock writeLock = writeLocks[Math.abs(notification.getKey().hashCode()) % concurrencyLevel];
                writeLock.lock();
                try {
                    heapEntry = notification.getValue();
                    pool.release(heapEntry.buffer);
                } finally {
                    writeLock.unlock();
                }

                allocatedOffHeapSize.addAndGet(-1 * heapEntry.normalizedSizeBytes);
                ms.decrement("offheap.size", heapEntry.normalizedSizeBytes);
                ms.decrement("offheap.wasted", (heapEntry.normalizedSizeBytes - heapEntry.sizeBytes));
                ms.decrement("offheap.count");

                if (cause == RemovalCause.SIZE) {
                    LOG.debug("Evicted from offheap: {}", notification.getKey());
                    ms.increment("offheap.evicted");
                }
            }

            // Throttle offheap usage for a while, if there are too many evictions
            if (size > evictionThreshold) {
                if (temporarySkipOffHeap.compareAndSet(false, true)) {
                    LOG.debug("Blocking offheap usage");
                    ms.increment("offheap.blocked");
                    blockedAt = System.currentTimeMillis();
                }
            } else if (blockedAt > 0 && System.currentTimeMillis() > (blockedAt + blockDurationMs)) {
                temporarySkipOffHeap.compareAndSet(true, false);
                blockedAt = 0L;
            }

            // Before cleaning the list again, sleep for a second
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while waiting in off heap cleaner: {}", e.getMessage());
            }
        }
    }
}
