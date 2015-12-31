package com.onshape.cache;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import org.springframework.validation.annotation.Validated;

import com.onshape.cache.exception.CacheException;

@Validated
public interface Cache {
    void put(@NotNull @Size(min = 1) String key, @NotNull @Size(min = 1) byte[] value) throws CacheException;

    byte[] get(@NotNull @Size(min = 1) String key) throws CacheException;

    boolean contains(@NotNull @Size(min = 1) String key) throws CacheException;

    void remove(@NotNull @Size(min = 1) String key) throws CacheException;
}
