package com.onshape.cache.onheap;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.metrics.CounterService;
import org.springframework.stereotype.Service;

import com.onshape.cache.OffHeap;
import com.onshape.cache.OnHeap;
import com.onshape.cache.exception.CacheException;

@Service
public class OnHeapImpl implements OnHeap, InitializingBean, HealthIndicator {
    private static final Logger LOG = LoggerFactory.getLogger(OnHeapImpl.class);

    @Value("${maxCacheEntries}")
    private int maxCacheEntries;

    private Set<String> keys;

    @Autowired
    private OffHeap offHeap;
    @Autowired
    private CounterService cs;

    @Override
    public void afterPropertiesSet() {
        LOG.info("Max cache entries: {}", maxCacheEntries);
        keys = Collections.synchronizedSet(new HashSet<>(maxCacheEntries));
        keys.add(""); // Causes the set to be initialized
        keys.remove("");
    }

    @Override
    public void put(String key, byte[] value) throws CacheException {
        put(key, value, value.length);
    }

    @Override
    public void put(String key, byte[] value, int length) throws CacheException {
        if (keys.remove(key)) {
            cs.decrement("cache.onheap.count");
            offHeap.remove(key);
        }

        offHeap.put(key, value, length);
        keys.add(key);
        cs.increment("cache.onheap.count");
    }

    @Override
    public ByteBuffer get(String key) throws CacheException {
        return offHeap.get(key);
    }

    @Override
    public boolean contains(String key) throws CacheException {
        return keys.contains(key);
    }

    @Override
    public void remove(String key) throws CacheException {
        keys.remove(key);
        cs.decrement("cache.onheap.count");
        offHeap.remove(key);
    }

    @Override
    public Health health() {
        NumberFormat formatter = new DecimalFormat("#0.00");
        return new Health.Builder().up()
            .withDetail("% full", formatter.format(((double) keys.size() / maxCacheEntries) * 100))
            .build();
    }
}
