package com.example.filterenrichment.enrich;

/**
 * Signals that the enrich concurrency bulkhead is saturated. The message is not failed or
 * DLQ'd: processing is aborted so the record is redelivered, and the consumer applies backpressure
 * (pauses partitions) until capacity frees up.
 */
public class BackpressureException extends RuntimeException {

    public BackpressureException(String message) {
        super(message);
    }
}
