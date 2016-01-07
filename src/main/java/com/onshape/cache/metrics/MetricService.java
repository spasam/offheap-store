package com.onshape.cache.metrics;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.endpoint.PublicMetrics;
import org.springframework.boot.actuate.metrics.Metric;
import org.springframework.stereotype.Service;

import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClientErrorHandler;

@Service
public class MetricService implements PublicMetrics, InitializingBean {
    private static final Logger LOG = LoggerFactory.getLogger(MetricService.class);

    private static final String COUNTER_PREFIX = "cache.counter.";
    private static final String GAUGE_PREFIX = "cache.gauge.";
    private static final String TIMER_PREFIX = "cache.timer.";

    @Value("${statsdEnabled}")
    private boolean statsdEnabled;
    @Value("${statsdHost}")
    private String statsdHost;
    @Value("${statsdPort}")
    private int statsdPort;
    @Value("${statsdPrefix}")
    private String statsdPrefix;

    private ConcurrentHashMap<String, Metric<?>> metrics = new ConcurrentHashMap<>();
    private NonBlockingStatsDClient client;

    @Override
    public void afterPropertiesSet() throws Exception {
        LOG.info("Statsd enabled: {}", statsdEnabled);
        LOG.info("Statsd host: {}", statsdHost);
        LOG.info("Statsd port: {}", statsdPort);
        LOG.info("Statsd prefix: {}", statsdPrefix);

        if (statsdEnabled) {
            client = new NonBlockingStatsDClient(statsdPrefix, statsdHost, statsdPort, new StatsDClientErrorHandler() {
                private volatile long lastReported;

                @Override
                public void handle(Exception exception) {
                    if (System.currentTimeMillis() - lastReported > 3600_000L) {
                        LOG.warn("Error sending metrics to statsd host", exception);
                        lastReported = System.currentTimeMillis();
                    }
                }
            });
        }
    }

    @Override
    public Collection<Metric<?>> metrics() {
        return metrics.values();
    }

    public void increment(String prefix) {
        increment(prefix, 1);
    }

    public void decrement(String prefix) {
        increment(prefix, -1);
    }

    public void decrement(String prefix, int decrement) {
        increment(prefix, -1 * decrement);
    }

    public void increment(String prefix, int increment) {
        if (increment == 0) {
            return;
        }
        Metric<?> metric = metrics.compute(prefix,
                (k, m) -> (m == null)
                        ? new Metric<Long>(COUNTER_PREFIX + prefix, (long) increment)
                        : m.increment(increment));
        if (client != null) {
            client.count(metric.getName(), metric.getValue().longValue());
        }
    }

    public void gauge(String prefix, double value) {
        Metric<?> metric = metrics.compute(GAUGE_PREFIX + prefix,
                (k, m) -> (m == null) ? new Metric<Double>(k, value) : m.set(value));
        if (client != null) {
            client.gauge(metric.getName(), metric.getValue().doubleValue());
        }
    }

    public void time(String prefix, long tookMs) {
        Metric<?> metric = metrics.compute(TIMER_PREFIX + prefix,
                (k, m) -> (m == null) ? new Metric<Long>(k, tookMs) : m.set(tookMs));
        if (client != null) {
            client.recordExecutionTime(metric.getName(), metric.getValue().longValue());
        }
    }

    public void reportMetrics(String action, long start) {
        time(action + ".time", System.currentTimeMillis() - start);
        increment(action);
    }

    public int reportMetrics(String action, String cacheName, long start) {
        long took = System.currentTimeMillis() - start;
        time(action + "." + cacheName + ".time", took);
        increment(action + "." + cacheName, 1);
        increment(action);

        return (int) Math.min(Integer.MAX_VALUE, took);
    }
}
