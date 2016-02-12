package com.onshape.cache.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * Invalid value exception.
 *
 * @author Seshu Pasam
 */
@ResponseStatus(value = HttpStatus.BAD_REQUEST, reason = "Invalid value")
public class InvalidValueException extends RuntimeException {
    private static final long serialVersionUID = 4721581625179581207L;

    public InvalidValueException(String message) {
        super(message);
    }
}
