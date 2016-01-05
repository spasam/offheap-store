package com.onshape.cache;

import java.nio.ByteBuffer;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import org.springframework.validation.annotation.Validated;

import com.onshape.cache.exception.CacheException;

@Validated
public interface DiskStore {
    void putAsync(@NotNull @Size(min = 1) String key, @NotNull @Size(min = 1) byte[] value) throws CacheException;

    ByteBuffer get(@NotNull @Size(min = 1) String key) throws CacheException;

    boolean contains(@NotNull @Size(min = 1) String key) throws CacheException;

    void removeAsync(@NotNull @Size(min = 1) String key) throws CacheException;
}
