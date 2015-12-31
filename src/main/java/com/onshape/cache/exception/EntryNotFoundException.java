package com.onshape.cache.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(value = HttpStatus.NOT_FOUND, reason = "No such entry")
public class EntryNotFoundException extends RuntimeException {
    private static final long serialVersionUID = -8613648319429506805L;
}
