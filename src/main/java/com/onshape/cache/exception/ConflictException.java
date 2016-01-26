package com.onshape.cache.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * Exception used when there is a conflict. Example: Triggering expiration cleanup task when one is already running.
 *
 * @author Seshu Pasam
 */
@ResponseStatus(value = HttpStatus.CONFLICT, reason = "Conflict")
public class ConflictException extends Exception {
    private static final long serialVersionUID = 3442558764268951607L;

    public ConflictException(String message) {
        super(message);
    }
}
