package com.onshape.cache.disk;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
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
import com.onshape.cache.exception.EntryNotFoundException;
import com.onshape.cache.metrics.MetricService;

/**
 * Disk store implementation.
 *
 * @author Seshu Pasam
 */
@Service
public class DiskStoreImpl implements DiskStore, InitializingBean, HealthIndicator {
    private static final Logger LOG = LoggerFactory.getLogger(DiskStoreImpl.class);
    private static final int TRANSFER_SIZE = 1024 * 1024;
    private static final String EXPIRE_ATTR = "e";
    private static final String LOST_FOUND = "lost+found";

    @Autowired
    private MetricService ms;

    @Value("${diskRoot}")
    private String root;

    /** Number of parts in root directory */
    private int rootNameCount;

    /** Where expiration is supported or not */
    private boolean expirationSupported;

    @Override
    public void afterPropertiesSet() throws IOException {
        Path rootDir = Paths.get(root);
        Path dir;
        if (Files.exists(rootDir)) {
            if (!Files.isDirectory(rootDir)) {
                throw new IOException("Not a directory: " + rootDir);
            }
            dir = rootDir.toRealPath();
        } else {
            dir = Files.createDirectories(rootDir);
        }

        root = dir.toString();
        rootNameCount = dir.getNameCount();

        LOG.info("Disk store root: {}", root);

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
                try (FileChannel fileChannel = raf.getChannel()) {
                    ByteBuffer buffer = fileChannel.map(MapMode.READ_ONLY, 0, fileChannel.size());

                    ms.reportMetrics("disk.get", start);
                    return buffer;
                }
            }
        } catch (IOException e) {
            LOG.error("Error reading file for entry: {}", key, e);
            throw new CacheException(e);
        }
    }

    @Override
    public List<String> list(String prefix) throws CacheException {
        long start = System.currentTimeMillis();
        Path path = Paths.get(root, prefix);
        if (Files.notExists(path)) {
            ms.increment("disk.list.miss");
            throw new EntryNotFoundException();
        }

        try {
            List<String> keys = new ArrayList<>();
            Files.walk(path, 1)
                            .filter((Path p) -> Files.isRegularFile(p))
                            .forEach((Path p) -> keys.add(p.getFileName().toString()));

            ms.reportMetrics("disk.list", start);
            return keys;
        } catch (IOException e) {
            LOG.error("Failed to list directory: {}", prefix, e);
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
        } catch (IOException e) {
            throw new CacheException(e);
        }
    }

    @Override
    public void put(String key, byte[] value, int expiresAtSecs, Function<String, Void> onSuccess)
                    throws CacheException {
        putAsync(key, value, expiresAtSecs, null);
        onSuccess.apply(key);
    }

    @Async
    @Override
    public void putAsync(String key, byte[] value, int expiresAtSecs, Function<String, Void> onError)
                    throws CacheException {
        long start = System.currentTimeMillis();
        Path path = Paths.get(root, key);
        Path parent = path.getParent();
        if (parent == null) {
            if (onError != null) {
                onError.apply(key);
            }
            throw new CacheException("Unable to find parent of path: " + path);
        }
        if (Files.notExists(parent)) {
            try {
                Files.createDirectories(parent);
            } catch (Throwable e) {
                if (onError != null) {
                    onError.apply(key);
                }
                throw new CacheException(e);
            }
        }

        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw")) {
            try (FileChannel fileChannel = raf.getChannel()) {
                int size = value.length;
                fileChannel.truncate(size);

                int length, offset = 0;
                while (offset < size) {
                    length = Math.min((size - offset), TRANSFER_SIZE);
                    ByteBuffer buffer = ByteBuffer.wrap(value, offset, length);
                    offset += fileChannel.write(buffer);
                }
            }
        } catch (Throwable e) {
            if (onError != null) {
                onError.apply(key);
            }
            try {
                Files.deleteIfExists(path);
            } catch (IOException ioe) {
                LOG.warn("Error deleting file for entry: {}", key, ioe);
            }
            throw new CacheException(e);
        }

        if (expirationSupported) {
            try {
                ByteBuffer buffer = ByteBuffer.allocate(4);
                buffer.putInt(expiresAtSecs);
                buffer.flip();

                UserDefinedFileAttributeView view = Files.getFileAttributeView(path, UserDefinedFileAttributeView.class);
                if (view != null) {
                    view.write(EXPIRE_ATTR, buffer);
                }
            } catch (IOException e) {
            }
        }

        ms.reportMetrics("disk.put", start);
    }

    @Override
    public void checkHierarchy(String prefix) throws CacheException {
        Path path = Paths.get(root, prefix);
        if (Files.notExists(path)) {
            throw new EntryNotFoundException("Not found: " + prefix);
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
        } catch (IOException e) {
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
                        (Runnable r) -> new Thread(r, "startup"));
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
                            .withDetail("% free", formatter
                                            .format((((double) fs.getUsableSpace() / fs.getTotalSpace()) * 100)))
                            .build();
        } catch (IOException e) {
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
        } catch (IOException e) {
            LOG.error("Error finding all cache names", e);
        }

        Collections.shuffle(caches);
        return caches;
    }

    private void remove(String key, Consumer<String> consumer) {
        try {
            Files.deleteIfExists(Paths.get(root, key));
        } catch (IOException e) {
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
        } catch (IOException e) {
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
        } catch (IOException e) {
            return 0;
        }
    }
}
