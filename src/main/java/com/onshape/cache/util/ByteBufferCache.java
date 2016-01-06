package com.onshape.cache.util;

import java.nio.ByteBuffer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ByteBufferCache extends ThreadLocal<ByteBuffer> {
    /** Threshold up to which thread local byte buffers will be used when reading files */
    @Value("${cachedBufferSize}")
    private int cachedBufferSize;

    @Override
    protected ByteBuffer initialValue() {
        return ByteBuffer.allocateDirect(cachedBufferSize);
    }

    public ByteBuffer get(int capacity) {
        ByteBuffer buf;
        if (capacity < cachedBufferSize) {
            buf = get();
        } else {
            buf = ByteBuffer.allocate(capacity);
        }

        buf.clear();
        return buf;
    }
}
