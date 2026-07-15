package com.example.filterenrichment.domain;

/**
 * Thrown when an input record is malformed or missing a mandatory field (input DLQ). The message
 * is the DLQ reason.
 */
public class InputParseException extends RuntimeException {

    public InputParseException(String message) {
        super(message);
    }

    public InputParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
