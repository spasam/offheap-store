package com.onshape.cache.buffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Simple byte buffer pool that provides methods to allocate and release {@code CompositeByteBuffer}
 *
 * @author Seshu Pasam
 */
@Component
public class BufferPool implements InitializingBean {
    private static final Logger LOG = LoggerFactory.getLogger(BufferPool.class);

    @Value("${offHeapChunkSizeBytes}")
    private int offHeapChunkSizeBytes;
    @Value("${maxOffHeapSizeBytes}")
    private long maxOffHeapSizeBytes;
    @Value("${maxOffHeapEntrySizeBytes}")
    private long maxOffHeapEntrySizeBytes;
    @Value("${server.tomcat.max-threads}")
    private int maxThreads;

    private List<ByteBuffer> buffers;

    @Override
    public void afterPropertiesSet() throws Exception {
        long usableOffHeapSizeBytes = maxOffHeapSizeBytes - (maxThreads * maxOffHeapEntrySizeBytes);
        int maxOffHeapEntries = (int) (usableOffHeapSizeBytes / offHeapChunkSizeBytes);
        LOG.info("Usable offheap size bytes: {}", usableOffHeapSizeBytes);
        LOG.info("Max offheap entries: {}", maxOffHeapEntries);

        // Preallocate buffers during startup
        buffers = new ArrayList<>(maxOffHeapEntries);
        for (int i = 0; i < maxOffHeapEntries; i++) {
            buffers.add(ByteBuffer.allocateDirect(offHeapChunkSizeBytes));
        }
    }

    /**
     * Returns a composite buffer. Data from {@code bytes} is copied to the composite byte buffer.
     *
     * @param bytes Data to copy.
     * @return Composite byte buffer. {@code null} if there aren't enough buffers.
     */
    public CompositeByteBuffer get(byte[] bytes) {
        int normalizedSizeBytes = (int) Math.ceil((double) bytes.length / offHeapChunkSizeBytes) * offHeapChunkSizeBytes;
        int count = (int) Math.ceil((double) normalizedSizeBytes / offHeapChunkSizeBytes);

        ByteBuffer[] bb = null;
        synchronized  (this) {
            if (count < buffers.size()) {
                bb = new ByteBuffer[count];
                for (int i = 0; i < count; i++) {
                    bb[i] = buffers.remove(buffers.size() - 1);
                }
            }
        }

        if (bb == null) {
            return null;
        }

        int offset = 0;
        for (int i = 0; i < count; i++) {
            int length = Math.min(bb[i].remaining(), (bytes.length - offset));
            bb[i].put(bytes, offset, length);
            bb[i].flip();
            offset += length;
        }

        return new CompositeByteBuffer(bb);
    }

    /**
     * Releases a composite byte buffer. All the chunks in the buffer are re-used.
     *
     * @param cbb Composite byte buffer to release.
     */
    public synchronized void release(CompositeByteBuffer cbb) {
        for (ByteBuffer buffer : cbb.buffers) {
            buffer.clear();
            buffers.add(buffer);
        }
    }
}
