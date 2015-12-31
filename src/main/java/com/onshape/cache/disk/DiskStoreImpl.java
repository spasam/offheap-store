package com.onshape.cache.disk;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.metrics.CounterService;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.onshape.cache.DiskStore;
import com.onshape.cache.exception.CacheException;
import com.onshape.cache.metrics.CacheMetrics;

@Service
public class DiskStoreImpl implements DiskStore, InitializingBean, HealthIndicator {
    private static final Logger LOG = LoggerFactory.getLogger(DiskStoreImpl.class);

    @Autowired
    private CounterService cs;
    @Autowired
    private CacheMetrics metrics;

    @Value("${diskRoot}")
    private String root;

    @Override
    public void afterPropertiesSet() throws IOException {
        LOG.info("Disk store root: {}", root);

        File dir = new File(root);
        if (!dir.exists()) {
            Files.createDirectories(dir.toPath());
        }
        if (!dir.isDirectory()) {
            throw new IOException("Not a directory: " + dir);
        }
    }

    @Override
    public boolean contains(String key) {
        long start = System.currentTimeMillis();
        try {
            return Files.exists(Paths.get(root, key));
        } finally {
            metrics.report("diskstore.head", null, start);
        }
    }

    @Override
    public byte[] get(String key) throws CacheException {
        Path path = Paths.get(root, key);
        if (Files.notExists(path)) {
            cs.increment("cache.diskstore.get.miss");
            return null;
        }

        long start = System.currentTimeMillis();
        try {
            long size = Files.size(path);
            byte[] bytes = new byte[(int) size];
            try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r")) {
                raf.readFully(bytes);
                metrics.report("diskstore.get", null, start);

                return bytes;
            }
        }
        catch (IOException e) {
            LOG.error("Error reading file for entry: {}", key, e);
            throw new CacheException(e);
        }
    }

    @Async
    @Override
    public void remove(String key) throws CacheException {
        long start = System.currentTimeMillis();
        try {
            Files.deleteIfExists(Paths.get(root, key));
            metrics.report("diskstore.delete", null, start);
        }
        catch (IOException e) {
            LOG.error("Error deleting file for entry: {}", key, e);
            throw new CacheException(e);
        }
    }

    @Async
    @Override
    public void put(String key, byte[] value) throws CacheException {
        long start = System.currentTimeMillis();
        Path path = Paths.get(root, key);
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw")) {
            raf.setLength(value.length);
            raf.write(value);
            metrics.report("diskstore.put", null, start);
        }
        catch (IOException e) {
            LOG.error("Failed to persist entry to disk: {}", key, e);
            try {
                Files.deleteIfExists(path);
            }
            catch (IOException ioe) {
                LOG.warn("Error deleting file for entry: {}", key, ioe);
            }
            throw new CacheException(e);
        }
    }

    @Override
    public Health health() {
        try {
            FileStore fs = Files.getFileStore(Paths.get(root));
            NumberFormat formatter = new DecimalFormat("#0.00");

            return new Health.Builder().up()
                .withDetail("% free", formatter.format((((double) fs.getUsableSpace() / fs.getTotalSpace()) * 100)))
                .build();
        }
        catch (IOException e) {
            LOG.error("Error getting file store information", e);
            return null;
        }
    }
}
