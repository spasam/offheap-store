package com.onshape.cache.util;

import java.nio.ByteBuffer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.onshape.cache.metrics.MetricService;

@Component
public class ByteBufferCache extends ThreadLocal<ByteBuffer> {
    /** Threshold up to which thread local direct byte buffers will be used when reading files */
    @Value("${cachedBufferSize}")
    private int cachedBufferSize;

    @Autowired
    private MetricService ms;

    @Override
    protected ByteBuffer initialValue() {
        ms.increment("byte.buffer.direct.count");
        ms.increment("byte.buffer.direct.size", cachedBufferSize);
        return ByteBuffer.allocateDirect(cachedBufferSize);
    }

    public ByteBuffer get(int capacity) {
        ByteBuffer buf;
        if (capacity <= cachedBufferSize) {
            buf = get();
        } else {
            ms.increment("byte.buffer.size", capacity);
            buf = ByteBuffer.allocate(capacity);
        }

        buf.clear();
        return buf;
    }
}
