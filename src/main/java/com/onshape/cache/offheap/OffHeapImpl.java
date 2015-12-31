package com.onshape.cache.offheap;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.metrics.CounterService;
import org.springframework.boot.actuate.metrics.GaugeService;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;
import com.onshape.cache.OffHeap;
import com.onshape.cache.exception.CacheException;
import com.onshape.cache.metrics.CacheMetrics;

import sun.misc.Unsafe;

@Service
public class OffHeapImpl implements OffHeap, InitializingBean {
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

    @Autowired
    private CounterService cs;
    @Autowired
    private GaugeService gs;
    @Autowired
    private CacheMetrics metrics;

    @Value("${maxOffHeapSizeBytes}")
    private long maxOffHeapSizeBytes;
    @Value("${maxOffHeapEntries}")
    private int maxOffHeapEntries;
    @Value("${concurrencyLocks}")
    private int concurrentLocks;

    private AtomicLong totalSize;
    private Lock[] readLocks;
    private Lock[] writeLocks;
    private Cache<String, HeapEntry> offHeapEntries;

    @Override
    public void afterPropertiesSet() throws Exception {
        LOG.info("Max offheap size bytes: {}", maxOffHeapSizeBytes);
        LOG.info("Max offheap entries: {}", maxOffHeapEntries);
        LOG.info("Concurrent locks: {}", concurrentLocks);

        totalSize = new AtomicLong(0);

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
            .weigher(new Weigher<String, HeapEntry>() {
                @Override
                public int weigh(String key, HeapEntry value) {
                    return value.sizeBytes;
                }
            })
            .removalListener(new RemovalListener<String, HeapEntry>() {
                @Override
                public void onRemoval(RemovalNotification<String, HeapEntry> notification) {
                    RemovalCause cause = notification.getCause();
                    Assert.isTrue(cause == RemovalCause.SIZE || cause == RemovalCause.EXPLICIT);

                    HeapEntry heapEntry = notification.getValue();
                    U.freeMemory(heapEntry.address);

                    gs.submit("cache.offheap.size", totalSize.addAndGet(-1 * heapEntry.sizeBytes));
                    cs.decrement("cache.offheap.count");

                    if (cause == RemovalCause.SIZE) {
                        LOG.info("Evicted from offheap: {}", notification.getKey());
                        cs.increment("cache.offheap.evicted");
                    }
                }
            })
            .build();
    }

    @Override
    public void put(String key, byte[] value) throws CacheException {
        int sizeBytes = value.length;
        long start = System.currentTimeMillis();

        long address = U.allocateMemory(sizeBytes);
        gs.submit("cache.offheap.size", totalSize.addAndGet(sizeBytes));

        U.copyMemory(value, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, address, sizeBytes);

        offHeapEntries.put(key, new HeapEntry(address, sizeBytes));

        metrics.report("offheap.put", null, start);
        cs.increment("cache.offheap.count");
    }

    @Override
    public byte[] get(String key) throws CacheException {
        byte[] value;
        long start = System.currentTimeMillis();

        Lock readLock = readLocks[Math.abs(key.hashCode()) % concurrentLocks];
        readLock.lock();
        try {
            HeapEntry heapEntry = offHeapEntries.getIfPresent(key);
            if (heapEntry == null) {
                cs.increment("cache.offheap.get.miss");
                return null;
            }

            value = new byte[heapEntry.sizeBytes];
            U.copyMemory(null, heapEntry.address, value, Unsafe.ARRAY_BYTE_BASE_OFFSET, heapEntry.sizeBytes);
        } finally {
            readLock.unlock();
        }
        metrics.report("offheap.get", null, start);

        return value;
    }

    @Override
    public void remove(String key) {
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
        throw new UnsupportedOperationException();
    }

    private static class HeapEntry {
        private final long address;
        private final int sizeBytes;

        private HeapEntry(long address, int sizeBytes) {
            this.address = address;
            this.sizeBytes = sizeBytes;
        }
    }
}
