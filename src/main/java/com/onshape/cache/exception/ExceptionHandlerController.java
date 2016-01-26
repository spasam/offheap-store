package com.onshape.cache.exception;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/**
 * Controller of exception advice. Translates exceptions to HTTP error codes.
 *
 * @author Seshu Pasam
 */
@ControllerAdvice
public class ExceptionHandlerController extends ResponseEntityExceptionHandler {
    @ResponseStatus(value = HttpStatus.INTERNAL_SERVER_ERROR)
    @ExceptionHandler({ CacheException.class })
    @ResponseBody
    ErrorInfo handleInternalError(HttpServletRequest req, HttpServletResponse res, Exception ex) {
        return new ErrorInfo(HttpStatus.INTERNAL_SERVER_ERROR.value(), "Internal server error: " + ex.getMessage());
    }

    @ResponseStatus(value = HttpStatus.CONFLICT)
    @ExceptionHandler({ ConflictException.class })
    @ResponseBody
    ErrorInfo handleConflictError(HttpServletRequest req, HttpServletResponse res, Exception ex) {
        return new ErrorInfo(HttpStatus.CONFLICT.value(), "Conflict: " + ex.getMessage());
    }

    @ResponseStatus(value = HttpStatus.NOT_FOUND)
    @ExceptionHandler({ EntryNotFoundException.class })
    void handleNotFound(HttpServletRequest req, HttpServletResponse res, Exception ex) {
    }
}
