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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
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

import com.onshape.cache.DiskStore;
import com.onshape.cache.exception.CacheException;
import com.onshape.cache.metrics.MetricService;
import com.onshape.cache.util.ByteBufferCache;

@Service
public class DiskStoreImpl implements DiskStore, InitializingBean, HealthIndicator {
    private static final Logger LOG = LoggerFactory.getLogger(DiskStoreImpl.class);
    private static final String EXPIRE_ATTR = "e";
    private static final String LOST_FOUND = "lost+found";

    @Autowired
    private ByteBufferCache bbc;
    @Autowired
    private MetricService ms;

    /** Threshold beyond which memory mapping will be used for reading from and writing to files */
    @Value("${diskRoot}")
    private String root;

    private int rootNameCount;
    private boolean expirationSupported;

    @Override
    public void afterPropertiesSet() throws IOException {
        Path dir = Paths.get(root).toRealPath();
        root = dir.toString();
        rootNameCount = dir.getNameCount();

        LOG.info("Disk store root: {}", root);

        if (Files.notExists(dir)) {
            Files.createDirectories(dir);
        }
        if (!Files.isDirectory(dir)) {
            throw new IOException("Not a directory: " + dir);
        }

        FileStore fs = Files.getFileStore(dir);
        if (!fs.supportsFileAttributeView(UserDefinedFileAttributeView.class)) {
            expirationSupported = true;
        } else {
            LOG.error("File store '{}' does not support user file attributes. Expiration support is disabled", fs);
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
    public void putAsync(String key, byte[] value, int expiresAtSecs) throws CacheException {
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

        if (expirationSupported) {
            try {
                ByteBuffer buffer = ByteBuffer.allocate(4);
                buffer.putInt(expiresAtSecs);
                buffer.flip();

                Files.getFileAttributeView(path, UserDefinedFileAttributeView.class).write(EXPIRE_ATTR, buffer);
            }
            catch (IOException e) {}
        }

        ms.reportMetrics("disk.put", start);
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
    public void removeHierarchyAsync(String prefix, Consumer<String> consumer) throws CacheException {
        Path path = Paths.get(root, prefix);
        try {
            Files.walk(path)
                .filter((Path p) -> Files.isRegularFile(p))
                .forEach((Path p) -> remove(getKey(p), consumer));
        }
        catch (IOException e) {
            throw new CacheException(e);
        }
    }

    @Override
    public void getKeys(BiConsumer<String, Integer> consumer) throws InterruptedException, ExecutionException {
        if (!expirationSupported) {
            return;
        }

        List<String> cacheNames = getCacheNames();
        cacheNames.remove(LOST_FOUND);
        if (cacheNames.size() < 1) {
            return;
        }

        ExecutorService es = Executors.newFixedThreadPool(cacheNames.size(),
                (Runnable r) -> new Thread(r, "kl"));
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (String cacheName : cacheNames) {
                futures.add(es.submit(() -> getKeys(cacheName, consumer)));
            }

            for (Future<?> f : futures) {
                f.get();
            }
        } finally {
            es.shutdown();
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
                .filter((Path p) -> Files.isDirectory(p))
                .forEach((Path p) -> caches.add(p.getFileName().toString()));
        }
        catch (IOException e) {
            LOG.error("Error finding all cache names", e);
        }

        Collections.shuffle(caches);
        return caches;
    }

    private void remove(String key, Consumer<String> consumer) {
        try {
            Files.deleteIfExists(Paths.get(root, key));
        }
        catch (IOException e) {
            LOG.error("Error deleting disk entry: {}", key, e);
        }

        consumer.accept(key);
        ms.increment("delete.expired");
    }

    private String getKey(Path path) {
        int pathNameCount = path.getNameCount();

        if (pathNameCount - rootNameCount == 4) {
            return path.getName(pathNameCount - 4)
                    + "/" + path.getName(pathNameCount - 3)
                    + "/" + path.getName(pathNameCount - 2)
                    + "/" + path.getName(pathNameCount - 1);
        }

        return path.getName(pathNameCount - 3)
                + "/" + path.getName(pathNameCount - 2)
                + "/" + path.getName(pathNameCount - 1);
    }

    private void getKeys(String cacheName, BiConsumer<String, Integer> consumer) {
        try {
            Files.walk(Paths.get(root, cacheName))
                .filter((Path p) -> Files.isRegularFile(p))
                .forEach((Path p) -> consumer.accept(getKey(p), getExpiresAt(p)));
        }
        catch (IOException e) {
            LOG.error("Error getting keys for cache: {}", cacheName, e);
            throw new RuntimeException(e);
        }
    }

    private int getExpiresAt(Path path) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(4);
            Files.getFileAttributeView(path, UserDefinedFileAttributeView.class).read(EXPIRE_ATTR, buffer);
            buffer.flip();

            return buffer.hasRemaining() ? buffer.getInt() : 0;
        }
        catch (IOException e) {
            return 0;
        }
    }
}
