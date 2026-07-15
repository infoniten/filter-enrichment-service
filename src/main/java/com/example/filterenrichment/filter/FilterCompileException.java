package com.example.filterenrichment.filter;

/**
 * Thrown when a subscription's RSQL filter cannot be compiled: unparseable, references an
 * unknown field, or traverses a collection. {@link #getReason()} is the machine code reported to the
 * Subscription Service {@code /internal/subscriptions/{id}/fail} endpoint.
 */
public class FilterCompileException extends RuntimeException {

    private final String reason;

    public FilterCompileException(String reason, String message) {
        super(message);
        this.reason = reason;
    }

    public String getReason() {
        return reason;
    }
}
