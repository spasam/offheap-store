package com.onshape.cache;

import java.nio.ByteBuffer;

public interface OffHeap {
    void put(String key, ByteBuffer buffer, boolean replace);

    void putAsync(String key, byte[] value);

    ByteBuffer get(String key);

    void removeAsync(String key);

    boolean isEnabled();

    boolean accepts(int length);
}
