package com.onshape.cache.metrics;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.endpoint.PublicMetrics;
import org.springframework.boot.actuate.metrics.GaugeService;
import org.springframework.boot.actuate.metrics.Metric;

public abstract class AbstractMetricsProvider implements PublicMetrics {
    @Autowired
    private GaugeService gs;

    private ConcurrentHashMap<String, Metric<?>> metrics = new ConcurrentHashMap<>();

    @Override
    public Collection<Metric<?>> metrics() {
        return metrics.values();
    }

    protected void increment(String suffix, int increment) {
        metrics.compute(suffix,
                (key, metric) -> (metric == null)
                        ? new Metric<Long>(suffix, (long) increment)
                        : metric.increment(increment));
    }

    protected void increment(String suffix, String cacheName, int increment) {
        increment(suffix + cacheName, increment);
    }

    protected void increment(String suffix) {
        increment(suffix, 1);
    }

    protected void decrement(String suffix, int decrement) {
        metrics.compute(suffix,
                (key, metric) -> (metric == null)
                        ? new Metric<Long>(suffix, -1L * decrement)
                        : metric.increment(-1 * decrement));
    }

    protected void decrement(String suffix) {
        decrement(suffix, 1);
    }

    protected int reportMetrics(String action, String cacheName, long start) {
        int took = 0;
        if (start > 0) {
            took = (int) (System.currentTimeMillis() - start);
            gs.submit(action + ".time", took);
        }
        if (cacheName != null) {
            increment(action, "." + cacheName, 1);
        }
        increment(action, 1);

        return took;
    }
}
