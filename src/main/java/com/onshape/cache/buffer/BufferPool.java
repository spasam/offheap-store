package com.onshape.cache.buffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

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

        buffers = new ArrayList<>(maxOffHeapEntries);
        for (int i = 0; i < maxOffHeapEntries; i++) {
            buffers.add(ByteBuffer.allocateDirect(offHeapChunkSizeBytes));
        }
    }

    public synchronized CompositeByteBuffer get(int count) {
        if (buffers.size() < count) {
            return null;
        }

        ByteBuffer[] bb = new ByteBuffer[count];
        for (int i = 0; i < count; i++) {
            bb[i] = buffers.remove(0);
        }

        return new CompositeByteBuffer(bb);
    }

    public synchronized void release(CompositeByteBuffer cbb) {
        for (ByteBuffer buffer : cbb.buffers) {
            buffer.clear();
            buffers.add(buffer);
        }
    }
}
