package com.onshape.cache.offheap;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
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
import com.onshape.cache.exception.CacheException;
import com.onshape.cache.metrics.AbstractMetricsProvider;
import com.onshape.cache.util.ByteBufferCache;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

@Service
public class OffHeapImpl extends AbstractMetricsProvider implements OffHeap, InitializingBean, HealthIndicator {
    private static final Logger LOG = LoggerFactory.getLogger(OffHeapImpl.class);
    private static final int DIRECT_ARENA_COUNT = 16;
    private static final int PAGE_SIZE = 8 * 1024;
    private static final int MAX_ORDER = 7; // PAGE_SIZE << MAX_ORDER = 1MB
    private static final int TINY_CACHE_SIZE = 512; // We don't use tiny cache
    private static final int SMALL_CACHE_SIZE = 256; // We don't use small cache
    private static final int NORMAL_CACHE_SIZE = 64 * 1024;

    private static class HeapEntry {
        private final int sizeBytes;
        private final int normalizedSizeBytes;
        private final ByteBuf buffer;

        private HeapEntry(int sizeBytes, ByteBuf buffer) {
            this.sizeBytes = sizeBytes;
            this.normalizedSizeBytes = normalizedSize(sizeBytes);
            this.buffer = buffer;
        }
    }

    @Autowired
    private ByteBufferCache byteBufferCache;

    @Value("${offHeapDisabled}")
    private boolean offHeapDisabled;
    @Value("${maxOffHeapSizeBytes}")
    private long maxOffHeapSizeBytes;
    @Value("${concurrencyLocks}")
    private int concurrentLocks;

    private int maxEntrySizeBytes;
    private AtomicLong allocatedOffHeapSize;
    private PooledByteBufAllocator allocator;

    private Lock[] readLocks;
    private Lock[] writeLocks;

    private Cache<String, HeapEntry> offHeapEntries;
    private List<RemovalNotification<String, HeapEntry>> lazyCleaningList = Collections.synchronizedList(
            new LinkedList<>());

    @Override
    public void afterPropertiesSet() throws Exception {
        if (offHeapDisabled) {
            return;
        }

        int maxOffHeapEntries = (int) (maxOffHeapSizeBytes / NORMAL_CACHE_SIZE);
        maxEntrySizeBytes = PAGE_SIZE << MAX_ORDER;

        LOG.info("Max offheap size bytes: {}", maxOffHeapSizeBytes);
        LOG.info("Max entry size bytes: {}", maxEntrySizeBytes);
        LOG.info("Max offheap entries: {}", maxOffHeapEntries);
        LOG.info("Concurrent locks: {}", concurrentLocks);

        allocatedOffHeapSize = new AtomicLong(0);
        allocator = new PooledByteBufAllocator(true, 0, DIRECT_ARENA_COUNT, PAGE_SIZE, MAX_ORDER,
                TINY_CACHE_SIZE, SMALL_CACHE_SIZE, NORMAL_CACHE_SIZE);

        readLocks = new Lock[concurrentLocks];
        writeLocks = new Lock[concurrentLocks];
        for (int i = 0; i < concurrentLocks; i++) {
            ReadWriteLock rwLock = new ReentrantReadWriteLock();
            readLocks[i] = rwLock.readLock();
            writeLocks[i] = rwLock.writeLock();
        }

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
    public void putAsync(String key, byte[] value, int length) throws CacheException {
        put(key, value, length);
    }

    @Override
    public void put(String key, byte[] value, int length) throws CacheException {
        if (offHeapDisabled) {
            return;
        }
        if (length > maxEntrySizeBytes) {
            increment("cache.offheap.entry.size.exceed");
            return;
        }

        long start = System.currentTimeMillis();
        int normalizedSizeBytes = normalizedSize(length);
        ByteBuf buf = allocateBuffer(normalizedSizeBytes);
        buf.writeBytes(value, 0, length);

        offHeapEntries.put(key, new HeapEntry(length, buf));

        allocatedOffHeapSize.addAndGet(normalizedSizeBytes);
        increment("cache.offheap.size", normalizedSizeBytes);
        increment("cache.offheap.wasted", (normalizedSizeBytes - length));
        increment("cache.offheap.count");
        reportMetrics("cache.offheap.put", null, start);
    }

    @Override
    public ByteBuffer get(String key) throws CacheException {
        if (offHeapDisabled) {
            return null;
        }

        long start = System.currentTimeMillis();
        ByteBuffer buffer;

        Lock readLock = readLocks[Math.abs(key.hashCode()) % concurrentLocks];
        readLock.lock();
        try {
            HeapEntry heapEntry = offHeapEntries.getIfPresent(key);
            if (heapEntry == null) {
                increment("cache.offheap.get.miss");
                return null;
            }

            buffer = byteBufferCache.get(heapEntry.sizeBytes);
            heapEntry.buffer.getBytes(0, buffer.array(), 0, heapEntry.sizeBytes);
            buffer.position(heapEntry.sizeBytes);
        } finally {
            readLock.unlock();
        }
        reportMetrics("cache.offheap.get", null, start);

        return buffer;
    }

    @Async
    @Override
    public void removeAsync(String key) {
        if (offHeapDisabled) {
            return;
        }

        long start = System.currentTimeMillis();

        Lock writeLock = writeLocks[Math.abs(key.hashCode()) % concurrentLocks];
        writeLock.lock();
        try {
            offHeapEntries.invalidate(key);
        } finally {
            writeLock.unlock();
        }

        reportMetrics("cache.offheap.delete", null, start);
    }

    @Override
    public boolean isEnabled() {
        return !offHeapDisabled;
    }

    @Override
    public boolean accepts(int sizeBytes) {
        return !offHeapDisabled && sizeBytes <= maxEntrySizeBytes;
    }

    @Override
    public Health health() {
        NumberFormat formatter = new DecimalFormat("#0.00");
        return new Health.Builder().up()
            .withDetail("% full", formatter.format(((double) allocatedOffHeapSize.get() / maxOffHeapSizeBytes) * 100))
            .build();
    }

    private void freeOffHeapEntries() {
        while (true) {
            while (!lazyCleaningList.isEmpty()) {
                RemovalNotification<String, HeapEntry> notification = lazyCleaningList.remove(0);
                RemovalCause cause = notification.getCause();
                Assert.isTrue(cause == RemovalCause.SIZE // Size exceeded
                        || cause == RemovalCause.EXPLICIT // Manually deleted by user
                        || cause == RemovalCause.REPLACED); // Entry is being replaced

                HeapEntry heapEntry;
                Lock writeLock = writeLocks[Math.abs(notification.getKey().hashCode()) % concurrentLocks];
                writeLock.lock();
                try {
                    heapEntry = notification.getValue();
                    heapEntry.buffer.release();
                } finally {
                    writeLock.unlock();
                }

                allocatedOffHeapSize.addAndGet(-1 * heapEntry.normalizedSizeBytes);
                decrement("cache.offheap.size", heapEntry.normalizedSizeBytes);
                decrement("cache.offheap.wasted", (heapEntry.normalizedSizeBytes - heapEntry.sizeBytes));
                decrement("cache.offheap.count");

                if (cause == RemovalCause.SIZE) {
                    LOG.info("Evicted from offheap: {}", notification.getKey());
                    increment("cache.offheap.evicted");
                }
            }

            // Before cleaning the list again, sleep for a second
            try {
                Thread.sleep(1000L);
            }
            catch (InterruptedException e) {
                LOG.warn("Interrupted while waiting in off heap cleaner: {}", e.getMessage());
            }
        }
    }

    private ByteBuf allocateBuffer(int normalizedSizeBytes) {
        if (normalizedSizeBytes == NORMAL_CACHE_SIZE) {
            return allocator.directBuffer(normalizedSizeBytes, normalizedSizeBytes);
        }

        int count = NORMAL_CACHE_SIZE / normalizedSizeBytes;
        CompositeByteBuf buf = allocator.compositeDirectBuffer(count);
        for (int i = 0; i < count; i++) {
            buf.addComponent(allocator.directBuffer(NORMAL_CACHE_SIZE, NORMAL_CACHE_SIZE));
        }

        return buf;
    }

    private static int normalizedSize(int sizeBytes) {
        return (int) Math.ceil(sizeBytes / NORMAL_CACHE_SIZE) * NORMAL_CACHE_SIZE;
    }
}
