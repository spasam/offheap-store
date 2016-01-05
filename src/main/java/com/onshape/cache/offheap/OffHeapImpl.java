package com.onshape.cache.offheap;

import java.lang.reflect.Field;
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
import org.springframework.boot.actuate.metrics.CounterService;
import org.springframework.boot.actuate.metrics.GaugeService;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalNotification;
import com.onshape.cache.OffHeap;
import com.onshape.cache.exception.CacheException;
import com.onshape.cache.metrics.CacheMetrics;
import com.onshape.cache.util.ByteBufferCache;

import sun.misc.Unsafe;

@Service
public class OffHeapImpl implements OffHeap, InitializingBean, HealthIndicator {
    private static final Logger LOG = LoggerFactory.getLogger(OffHeapImpl.class);

    private static final Unsafe U;
    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            U = (Unsafe) field.get(null);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static class HeapEntry {
        private final long address;
        private final int sizeBytes;

        private HeapEntry(long address, int sizeBytes) {
            this.address = address;
            this.sizeBytes = sizeBytes;
        }
    }

    @Autowired
    private ByteBufferCache bbc;
    @Autowired
    private CounterService cs;
    @Autowired
    private GaugeService gs;
    @Autowired
    private CacheMetrics metrics;

    @Value("${offHeapDisabled}")
    private boolean offHeapDisabled;
    @Value("${maxOffHeapSizeBytes}")
    private long maxOffHeapSizeBytes;
    @Value("${maxOffHeapEntries}")
    private int maxOffHeapEntries;
    @Value("${concurrencyLocks}")
    private int concurrentLocks;

    private AtomicLong allocatedOffHeapSize;

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

        LOG.info("Max offheap size bytes: {}", maxOffHeapSizeBytes);
        LOG.info("Max offheap entries: {}", maxOffHeapEntries);
        LOG.info("Concurrent locks: {}", concurrentLocks);

        allocatedOffHeapSize = new AtomicLong(0);

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
            .weigher((String key, HeapEntry value) -> value.sizeBytes)
            .removalListener((RemovalNotification<String, HeapEntry> rn) -> lazyCleaningList.add(rn))
            .build();

        Executors.newSingleThreadExecutor((Runnable r) -> new Thread(r, "oh-cleaner"))
            .submit(() -> freeOffHeapEntries());
    }

    @Override
    public void put(String key, byte[] value) throws CacheException {
        put(key, value, value.length);
    }

    @Override
    public void put(String key, byte[] value, int length) throws CacheException {
        if (offHeapDisabled) {
            return;
        }

        long start = System.currentTimeMillis();

        long address = U.allocateMemory(length);
        U.copyMemory(value, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, address, length);

        offHeapEntries.put(key, new HeapEntry(address, length));

        metrics.report("offheap.put", null, start);
        gs.submit("cache.offheap.size", allocatedOffHeapSize.addAndGet(length));
        cs.increment("cache.offheap.count");
    }

    @Override
    public ByteBuffer get(String key) throws CacheException {
        if (offHeapDisabled) {
            return null;
        }

        long start = System.currentTimeMillis();

        Lock readLock = readLocks[Math.abs(key.hashCode()) % concurrentLocks];
        readLock.lock();
        try {
            HeapEntry heapEntry = offHeapEntries.getIfPresent(key);
            if (heapEntry == null) {
                cs.increment("cache.offheap.get.miss");
                return null;
            }

            ByteBuffer buffer = bbc.get(heapEntry.sizeBytes);
            U.copyMemory(null, heapEntry.address, buffer.array(), Unsafe.ARRAY_BYTE_BASE_OFFSET, heapEntry.sizeBytes);
            buffer.position(heapEntry.sizeBytes);

            return buffer;
        } finally {
            readLock.unlock();
            metrics.report("offheap.get", null, start);
        }
    }

    @Override
    public void remove(String key) {
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
        metrics.report("offheap.delete", null, start);
    }

    @Override
    public boolean contains(String key) throws CacheException {
        return offHeapEntries.getIfPresent(key) != null;
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
                Assert.isTrue(cause == RemovalCause.SIZE || cause == RemovalCause.EXPLICIT);

                int sizeBytes;
                Lock writeLock = writeLocks[Math.abs(notification.getKey().hashCode()) % concurrentLocks];
                writeLock.lock();
                try {
                    HeapEntry heapEntry = notification.getValue();
                    U.freeMemory(heapEntry.address);
                    sizeBytes = heapEntry.sizeBytes;
                } finally {
                    writeLock.unlock();
                }

                gs.submit("cache.offheap.size", allocatedOffHeapSize.addAndGet(-1 * sizeBytes));
                cs.decrement("cache.offheap.count");

                if (cause == RemovalCause.SIZE) {
                    LOG.info("Evicted from offheap: {}", notification.getKey());
                    cs.increment("cache.offheap.evicted");
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
}
