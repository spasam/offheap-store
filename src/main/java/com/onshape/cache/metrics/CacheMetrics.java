package com.onshape.cache.metrics;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.endpoint.PublicMetrics;
import org.springframework.boot.actuate.metrics.CounterService;
import org.springframework.boot.actuate.metrics.GaugeService;
import org.springframework.boot.actuate.metrics.Metric;
import org.springframework.stereotype.Component;

@Component
public class CacheMetrics implements PublicMetrics {
    private static final String METRIC_PREFIX = "cache.";

    private ConcurrentHashMap<String, Metric<?>> metrics = new ConcurrentHashMap<>();

    @Autowired
    private CounterService cs;
    @Autowired
    private GaugeService gs;

    @Override
    public Collection<Metric<?>> metrics() {
        return metrics.values();
    }

    public void increment(String suffix, String c, int increment) {
        metrics.compute(suffix + c,
                (key, metric) -> (metric == null)
                        ? new Metric<Long>(METRIC_PREFIX + suffix + c, (long) increment)
                        : metric.increment(increment));
    }

    public int report(String action, String c, long start) {
        int took = 0;
        if (start > 0) {
            took = (int) (System.currentTimeMillis() - start);
            gs.submit(METRIC_PREFIX + action + ".time", took);
        }
        if (c != null) {
            cs.increment(METRIC_PREFIX + action + "." + c);
        }
        cs.increment(METRIC_PREFIX + action);

        return took;
    }
}
