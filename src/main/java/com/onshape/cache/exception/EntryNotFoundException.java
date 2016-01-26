package com.onshape.cache.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * Entry not found exception.
 *
 * @author Seshu Pasam
 */
@ResponseStatus(value = HttpStatus.NOT_FOUND, reason = "Not found")
public class EntryNotFoundException extends RuntimeException {
    private static final long serialVersionUID = -8613648319429506805L;

    public EntryNotFoundException() {
        super();
    }

    public EntryNotFoundException(String message) {
        super(message);
    }
}
