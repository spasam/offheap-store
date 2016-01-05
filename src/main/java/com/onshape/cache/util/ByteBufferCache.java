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
        return ByteBuffer.allocate(cachedBufferSize);
    }

    public ByteBuffer get(int capacity) {
        if (capacity < cachedBufferSize) {
            return get();
        }

        return ByteBuffer.allocate(capacity);
    }
}
