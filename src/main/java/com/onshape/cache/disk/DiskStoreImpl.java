package com.onshape.cache.disk;

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
import java.util.function.Function;

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

        Path dir = Paths.get(root);
        if (Files.notExists(dir)) {
            Files.createDirectories(dir);
        }
        if (!Files.isDirectory(dir)) {
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
            throw new CacheException(e);
        }
    }

    @Async
    @Override
    public void putAsync(String key, byte[] value) throws CacheException {
        long start = System.currentTimeMillis();
        Path path = Paths.get(root, key);
        Path parent = path.getParent();
        if (Files.notExists(path.getParent())) {
            try {
                Files.createDirectories(parent);
            }
            catch (IOException e) {
                throw new CacheException(e);
            }
        }

        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw")) {
            raf.setLength(0);
            raf.getChannel().write(ByteBuffer.wrap(value));
            ms.reportMetrics("disk.put", start);
        }
        catch (IOException e) {
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
    public void removeHierarchy(String prefix, Function<String, Void> function) throws CacheException {
        Path path = Paths.get(root, prefix);
        if (Files.notExists(path)) {
            throw new CacheException("Not found: " + prefix);
        }
        if (!Files.isDirectory(path)) {
            throw new CacheException("Invalid entry: " + prefix);
        }

        removeHierarchyAsync(path, function);
    }

    @Async
    private void removeHierarchyAsync(Path path, Function<String, Void> function) throws CacheException {
        try {
            Files.list(path).forEach((Path p) -> function.apply(p.toFile().getName()));
        }
        catch (IOException e) {
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
