package com.onshape.cache;

public interface OnHeap {
    void put(String key);

    boolean contains(String key);

    boolean remove(String key);
}
