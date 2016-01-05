package com.onshape.cache;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import com.onshape.cache.exception.CacheException;

public interface OffHeap extends Cache {
    void put(@NotNull @Size(min = 1) String key, @NotNull @Size(min = 1) byte[] value, @Min(1) int length)
            throws CacheException;
}
