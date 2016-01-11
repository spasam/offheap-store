package com.onshape.cache.onheap;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.onshape.cache.OnHeap;
import com.onshape.cache.metrics.MetricService;

@Service
public class OnHeapImpl implements OnHeap, InitializingBean, HealthIndicator {
    private static final Logger LOG = LoggerFactory.getLogger(OnHeapImpl.class);

    @Value("${maxCacheEntries}")
    private int maxCacheEntries;
    @Value("${server.tomcat.max-threads}")
    private int concurrencyLevel;

    @Autowired
    private MetricService ms;

    private Map<String, Integer> cache;

    @Override
    public void afterPropertiesSet() {
        LOG.info("Max cache entries: {}", maxCacheEntries);

        cache = new ConcurrentHashMap<>(maxCacheEntries, 1.0f, concurrencyLevel);
        cache.put("", 0); // Force create the buckets during startup
        cache.remove("");
    }

    @Override
    public void put(String key, int expiresAtSecs) {
        if (cache.remove(key) == null) {
            ms.increment("onheap.count");
        }
        cache.put(key, expiresAtSecs);
    }

    @Override
    public boolean contains(String key) {
        return cache.containsKey(key);
    }

    @Override
    public boolean remove(String key) {
        if (cache.remove(key) != null) {
            ms.decrement("onheap.count");
            return true;
        }

        return false;
    }

    @Override
    public Health health() {
        NumberFormat formatter = new DecimalFormat("#0.00");
        return new Health.Builder().up()
            .withDetail("% full", formatter.format(((double) cache.size() / maxCacheEntries) * 100))
            .build();
    }

    @Async
    @Override
    public void cleanupExpired(Consumer<String> consumer) {
        int now = (int) (System.currentTimeMillis() / 1000L);
        for (Map.Entry<String, Integer> entry : cache.entrySet()) {
            int expiresAtSecs = entry.getValue();
            if (expiresAtSecs != 0 && expiresAtSecs < now) {
                String key = entry.getKey();
                cache.remove(key);
                consumer.accept(key);
            }
        }
    }
}
