package com.example.filterenrichment.enrich;

/**
 * Failure of an Object Enrich Service call (§27). The {@link Kind} drives handling: RETRYABLE errors
 * (429/502/503/504, timeouts, connection reset, circuit open) are retried with backoff; NOT_FOUND
 * (404) and NON_RETRYABLE (400 / invalid outputField / malformed) are not retried. When retries are
 * exhausted the message is routed to the Enrichment DLQ (§29).
 */
public class EnrichException extends RuntimeException {

    public enum Kind {
        RETRYABLE,
        NOT_FOUND,
        NON_RETRYABLE
    }

    private final Kind kind;

    public EnrichException(Kind kind, String message) {
        super(message);
        this.kind = kind;
    }

    public EnrichException(Kind kind, String message, Throwable cause) {
        super(message, cause);
        this.kind = kind;
    }

    public Kind kind() {
        return kind;
    }

    public boolean isRetryable() {
        return kind == Kind.RETRYABLE;
    }
}
