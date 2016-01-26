package com.onshape.cache.exception;

public class ErrorInfo {
    private int status;
    private String message;

    public ErrorInfo() {
    }

    public ErrorInfo(int status, String message) {
        this.status = status;
        this.message = message;
    }

    public int getStatus() {
        return status;
    }

    public String getMessage() {
        return message;
    }
}
