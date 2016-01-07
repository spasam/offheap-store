package com.onshape.cache.disk;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.onshape.cache.DiskStore;
import com.onshape.cache.exception.CacheException;
import com.onshape.cache.metrics.MetricService;
import com.onshape.cache.util.ByteBufferCache;

@Service
public class DiskStoreImpl implements DiskStore, InitializingBean, HealthIndicator {
    private static final Logger LOG = LoggerFactory.getLogger(DiskStoreImpl.class);

    @Autowired
    private ByteBufferCache bbc;
    @Autowired
    private MetricService ms;

    /** Threshold beyond which memory mapping will be used for reading from and writing to files */
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
            ms.reportMetrics("disk.head", start);
        }
    }

    @Override
    public ByteBuffer get(String key) throws CacheException {
        long start = System.currentTimeMillis();
        Path path = Paths.get(root, key);
        if (Files.notExists(path)) {
            ms.increment("disk.get.miss");
            return null;
        }

        try {
            try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r")) {
                FileChannel fileChannel = raf.getChannel();
                int size = (int) fileChannel.size();

                ByteBuffer buffer = bbc.get(size);
                fileChannel.read(buffer);
                buffer.flip();

                ms.reportMetrics("disk.get", start);
                return buffer;
            }
        }
        catch (IOException e) {
            LOG.error("Error reading file for entry: {}", key, e);
            throw new CacheException(e);
        }
    }

    @Async
    @Override
    public void removeAsync(String key) throws CacheException {
        long start = System.currentTimeMillis();
        try {
            Files.deleteIfExists(Paths.get(root, key));
            ms.reportMetrics("disk.delete", start);
        }
        catch (IOException e) {
            LOG.error("Error deleting file for entry: {}", key, e);
            throw new CacheException(e);
        }
    }

    @Async
    @Override
    public void putAsync(String key, byte[] value) throws CacheException {
        long start = System.currentTimeMillis();
        Path path = Paths.get(root, key);
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw")) {
            raf.setLength(0);
            raf.getChannel().write(ByteBuffer.wrap(value));
            ms.reportMetrics("disk.put", start);
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
