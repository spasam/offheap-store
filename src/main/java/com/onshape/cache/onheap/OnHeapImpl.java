package com.onshape.cache.onheap;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Service;

import com.onshape.cache.OnHeap;
import com.onshape.cache.exception.CacheException;
import com.onshape.cache.metrics.AbstractMetricsProvider;

@Service
public class OnHeapImpl extends AbstractMetricsProvider implements OnHeap, InitializingBean, HealthIndicator {
    private static final Logger LOG = LoggerFactory.getLogger(OnHeapImpl.class);

    @Value("${maxCacheEntries}")
    private int maxCacheEntries;

    private Set<String> keys;

    @Override
    public void afterPropertiesSet() {
        LOG.info("Max cache entries: {}", maxCacheEntries);

        keys = new HashSet<>(maxCacheEntries);
        keys.add(""); // Causes the set to be initialized
        keys.remove("");
    }

    @Override
    public synchronized void put(String key) {
        if (!keys.remove(key)) {
            increment("cache.onheap.count");
        }
        keys.add(key);
    }

    @Override
    public synchronized boolean contains(String key) throws CacheException {
        return keys.contains(key);
    }

    @Override
    public synchronized boolean remove(String key) throws CacheException {
        if (keys.remove(key)) {
            decrement("cache.onheap.count");
            return true;
        }

        return false;
    }

    @Override
    public Health health() {
        NumberFormat formatter = new DecimalFormat("#0.00");
        return new Health.Builder().up()
            .withDetail("% full", formatter.format(((double) keys.size() / maxCacheEntries) * 100))
            .build();
    }
}
