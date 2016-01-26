package com.onshape.cache.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * Generic cache exception.
 *
 * @author Seshu Pasam
 */
@ResponseStatus(value = HttpStatus.INTERNAL_SERVER_ERROR, reason = "Internal error")
public class CacheException extends Exception {
    private static final long serialVersionUID = 5632237203461348136L;

    public CacheException() {
        super();
    }

    public CacheException(String message) {
        super(message);
    }

    public CacheException(Throwable t) {
        super(t);
    }
}
