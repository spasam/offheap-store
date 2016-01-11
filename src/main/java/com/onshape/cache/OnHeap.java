package com.onshape.cache;

import java.util.function.Consumer;

public interface OnHeap {
    void put(String key, int expiresAtSecs);

    boolean contains(String key);

    boolean remove(String key);

    void cleanupExpired(Consumer<String> consumer);
}
