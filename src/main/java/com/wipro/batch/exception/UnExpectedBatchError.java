package com.wipro.batch.exception;

public class UnExpectedBatchError extends RuntimeException {

    public UnExpectedBatchError(String message, Throwable cause) {
        super(message, cause);
    }

    public UnExpectedBatchError(String message) {
        super(message);
    }
}
