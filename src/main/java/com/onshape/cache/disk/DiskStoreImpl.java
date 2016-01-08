package com.onshape.cache.disk;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.UserDefinedFileAttributeView;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
    private static final String EXPIRE_ATTR = "e";
    private static final String LOST_FOUND = "lost+found";
    private static final String DISK_SCAVENGER_PREFIX = "ds-";

    @Autowired
    private ByteBufferCache bbc;
    @Autowired
    private MetricService ms;

    /** Threshold beyond which memory mapping will be used for reading from and writing to files */
    @Value("${diskRoot}")
    private String root;
    @Value("${diskScavengerIntervalSecs}")
    private int diskScavengerIntervalSecs;

    private boolean expirationSupported;

    @Override
    public void afterPropertiesSet() throws IOException {
        LOG.info("Disk store root: {}", root);
        LOG.info("Disk scavenger interval secs: {}", diskScavengerIntervalSecs);

        Path dir = Paths.get(root);
        if (Files.notExists(dir)) {
            Files.createDirectories(dir);
        }
        if (!Files.isDirectory(dir)) {
            throw new IOException("Not a directory: " + dir);
        }

        FileStore fs = Files.getFileStore(dir.toRealPath());
        if (!fs.supportsFileAttributeView(UserDefinedFileAttributeView.class)) {
            expirationSupported = true;
        } else {
            LOG.error("File store '{}' does not support user file attributes. Expiration support is disabled", fs);
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
    public void putAsync(String key, byte[] value, int expireSecs) throws CacheException {
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

        ms.reportMetrics("disk.put", start);

        if (expirationSupported) {
            try {
                ByteBuffer buffer = ByteBuffer.allocate(4);
                buffer.putInt((int) (System.currentTimeMillis() / 1000L) + expireSecs);
                buffer.flip();

                Files.getFileAttributeView(path, UserDefinedFileAttributeView.class).write(EXPIRE_ATTR, buffer);
            }
            catch (IOException e) {
                throw new CacheException(e);
            }
        }
    }

    @Override
    public void checkHierarchy(String prefix) throws CacheException {
        Path path = Paths.get(root, prefix);
        if (Files.notExists(path)) {
            throw new CacheException("Not found: " + prefix);
        }
        if (!Files.isDirectory(path)) {
            throw new CacheException("Invalid entry: " + prefix);
        }
    }

    @Async
    @Override
    public void removeHierarchyAsync(String prefix, Function<String, Void> deleteFunction) throws CacheException {
        Path path = Paths.get(root, prefix);
        try {
            Files.list(path).forEach((Path p) -> deleteFunction.apply(getKey(p)));
        }
        catch (IOException e) {
            throw new CacheException(e);
        }
    }

    @Async
    @Override
    public void startScavengerAsync(Function<String, Void> deleteFunction) {
        if (!expirationSupported) {
            return;
        }

        while (true) {
            List<String> cacheNames = getCacheNames();
            cacheNames.remove(LOST_FOUND);

            if (cacheNames.size() > 0) {
                ExecutorService es = Executors.newFixedThreadPool(cacheNames.size(),
                        (Runnable r) -> new Thread(r, DISK_SCAVENGER_PREFIX));
                try {
                    int now = (int) (System.currentTimeMillis() / 1000L);
                    for (String cacheName : cacheNames) {
                        es.submit(() -> scavengeCache(cacheName, now, deleteFunction));
                    }

                    Thread.sleep(5000L);
                }
                catch (InterruptedException e) {
                    LOG.error("Interrupted after launching cache scavengers", e);
                } finally {
                    es.shutdown();
                }
            }

            try {
                Thread.sleep(diskScavengerIntervalSecs * 1000L);
            }
            catch (InterruptedException e) {
                LOG.warn("Scavenger interrupted while sleeping", e);
            }
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

    private List<String> getCacheNames() {
        List<String> caches = new ArrayList<>();
        try {
            Files.list(Paths.get(root))
                .forEach((Path p) -> caches.add(p.getFileName().toString()));
        }
        catch (IOException e) {
            LOG.error("Error finding all cache names", e);
        }

        Collections.shuffle(caches);
        return caches;
    }

    private void scavengeCache(String cacheName, int now, Function<String, Void> deleteFunction) {
        LOG.info("Finding expired entries in: {}", cacheName);
        ByteBuffer buffer = ByteBuffer.allocate(4);
        try {
            Files.walk(Paths.get(root, cacheName))
                .filter((Path p) -> isExpired(p, now, buffer))
                .forEach((Path p) -> removeExpired(getKey(p), deleteFunction));
        }
        catch (IOException e) {
            LOG.error("Error walking cache entries in: {}", cacheName, e);
        }
    }

    private boolean isExpired(Path path, int now, ByteBuffer buffer) {
        try {
            if (!Files.isDirectory(path)) {
                buffer.clear();
                Files.getFileAttributeView(path, UserDefinedFileAttributeView.class).read(EXPIRE_ATTR, buffer);
                buffer.flip();

                return buffer.hasRemaining() && buffer.getInt() < now;
            }
        }
        catch (IOException e) {
            // Does not have expire attribute. Keep the entry. Don't log, will be noisy
        }

        return false;
    }

    private void removeExpired(String key, Function<String, Void> deleteFunction) {
        try {
            Files.deleteIfExists(Paths.get(root, key));
        }
        catch (IOException e) {
            LOG.error("Error delete expired disk entry: {}", key, e);
        }

        deleteFunction.apply(key);
        ms.increment("delete.expired");
    }

    private String getKey(Path path) {
        int count = path.getNameCount();
        if (count < 4) {
            return null;
        }

        return path.getName(count - 4)
                + "/" + path.getName(count - 3)
                + "/" + path.getName(count - 2)
                + "/" + path.getName(count - 1);
    }
}
