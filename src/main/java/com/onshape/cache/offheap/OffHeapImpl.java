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
import com.onshape.cache.metrics.MetricService;
import com.onshape.cache.util.ByteBufferCache;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

@Service
public class OffHeapImpl implements OffHeap, InitializingBean, HealthIndicator {
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

        @Override
        public int hashCode() {
            int addr = buffer.hasMemoryAddress() ? (int) (buffer.memoryAddress() % Integer.MAX_VALUE) : 0;
            int result = 31 + addr;
            result = 31 * result + normalizedSizeBytes;
            result = 31 * result + sizeBytes;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }

            HeapEntry other = (HeapEntry) obj;
            if (buffer == null) {
                if (other.buffer != null) {
                    return false;
                }
            } else if (buffer != other.buffer) {
                return false;
            }
            if (normalizedSizeBytes != other.normalizedSizeBytes) {
                return false;
            }
            if (sizeBytes != other.sizeBytes) {
                return false;
            }
            return true;
        }
    }

    @Autowired
    private ByteBufferCache byteBufferCache;
    @Autowired
    private MetricService ms;

    @Value("${offHeapDisabled}")
    private boolean offHeapDisabled;
    @Value("${maxOffHeapSizeBytes}")
    private long maxOffHeapSizeBytes;
    @Value("${server.tomcat.max-threads}")
    private int concurrencyLevel;

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

        LOG.info("Offheap block size bytes: {}", NORMAL_CACHE_SIZE);
        LOG.info("Max offheap size bytes: {}", maxOffHeapSizeBytes);
        LOG.info("Max offheap entry size bytes: {}", maxEntrySizeBytes);
        LOG.info("Max offheap entries: {}", maxOffHeapEntries);
        LOG.info("Concurrent locks: {}", concurrencyLevel);

        allocatedOffHeapSize = new AtomicLong(0);
        allocator = new PooledByteBufAllocator(true, 0, DIRECT_ARENA_COUNT, PAGE_SIZE, MAX_ORDER,
                TINY_CACHE_SIZE, SMALL_CACHE_SIZE, NORMAL_CACHE_SIZE);

        readLocks = new Lock[concurrencyLevel];
        writeLocks = new Lock[concurrencyLevel];
        for (int i = 0; i < concurrencyLevel; i++) {
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
    public void putAsync(String key, byte[] value) {
        put(key, ByteBuffer.wrap(value), true);
    }

    @Override
    public void put(String key, ByteBuffer buffer, boolean replace) {
        if (offHeapDisabled) {
            return;
        }

        int length = buffer.limit();
        if (length > maxEntrySizeBytes) {
            ms.increment("offheap.entry.size.exceed");
            return;
        }

        if (!replace && offHeapEntries.getIfPresent(key) != null) {
            return;
        }

        long start = System.currentTimeMillis();
        int normalizedSizeBytes = normalizedSize(length);

        ByteBuf buf = allocateBuffer(normalizedSizeBytes);
        buf.writeBytes(buffer);
        buffer.flip();

        // If we are replacing the value, removal notification will be called with old value
        // Removal notification will take care of cleaning old heap entry
        offHeapEntries.put(key, new HeapEntry(length, buf));

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
        ByteBuffer buffer;

        Lock readLock = readLocks[Math.abs(key.hashCode()) % concurrencyLevel];
        readLock.lock();
        try {
            HeapEntry heapEntry = offHeapEntries.getIfPresent(key);
            if (heapEntry == null) {
                ms.increment("offheap.get.miss");
                return null;
            }

            buffer = byteBufferCache.get(heapEntry.sizeBytes);
            buffer.limit(heapEntry.sizeBytes);

            heapEntry.buffer.getBytes(0, buffer);
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
                Lock writeLock = writeLocks[Math.abs(notification.getKey().hashCode()) % concurrencyLevel];
                writeLock.lock();
                try {
                    heapEntry = notification.getValue();
                    heapEntry.buffer.release();
                } finally {
                    writeLock.unlock();
                }

                allocatedOffHeapSize.addAndGet(-1 * heapEntry.normalizedSizeBytes);
                ms.decrement("offheap.size", heapEntry.normalizedSizeBytes);
                ms.decrement("offheap.wasted", (heapEntry.normalizedSizeBytes - heapEntry.sizeBytes));
                ms.decrement("offheap.count");

                if (cause == RemovalCause.SIZE) {
                    LOG.info("Evicted from offheap: {}", notification.getKey());
                    ms.increment("offheap.evicted");
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

        int count = (int) Math.ceil((double) normalizedSizeBytes / NORMAL_CACHE_SIZE);
        CompositeByteBuf buf = allocator.compositeDirectBuffer(count);
        for (int i = 0; i < count; i++) {
            buf.addComponent(allocator.directBuffer(NORMAL_CACHE_SIZE, NORMAL_CACHE_SIZE));
        }

        return buf;
    }

    private static int normalizedSize(int sizeBytes) {
        return (int) Math.ceil((double) sizeBytes / NORMAL_CACHE_SIZE) * NORMAL_CACHE_SIZE;
    }
}
