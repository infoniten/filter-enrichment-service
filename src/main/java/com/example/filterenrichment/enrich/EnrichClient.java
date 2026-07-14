package com.example.filterenrichment.enrich;

import com.example.filterenrichment.config.FilterEnrichmentProperties;
import com.example.filterenrichment.metrics.Metrics;
import com.fasterxml.jackson.databind.JsonNode;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadFullException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

/**
 * Client for the Object Enrich Service (§12/§16/§27/§33). One HTTP call per input record (no
 * micro-batching): {@code GET .../{objectClass}} for a single object, {@code POST .../revisions} for
 * a before/after pair. Calls are guarded by a bulkhead (bounded concurrency), a circuit breaker
 * (fail-fast when the upstream is down), retried with exponential backoff + jitter, and pooled.
 */
@Component
public class EnrichClient {

    private static final Logger log = LoggerFactory.getLogger(EnrichClient.class);

    private final RestClient restClient;
    private final CircuitBreaker circuitBreaker;
    private final Bulkhead bulkhead;
    private final Metrics metrics;
    private final FilterEnrichmentProperties.Retry retry;

    public EnrichClient(@Qualifier("enrichRestClient") RestClient restClient,
                        @Qualifier("enrichCircuitBreaker") CircuitBreaker circuitBreaker,
                        @Qualifier("enrichBulkhead") Bulkhead bulkhead,
                        Metrics metrics,
                        FilterEnrichmentProperties props) {
        this.restClient = restClient;
        this.circuitBreaker = circuitBreaker;
        this.bulkhead = bulkhead;
        this.metrics = metrics;
        this.retry = props.getRetry();
    }

    /** Enriches a single object by globalId (§12). Returns the enriched payload. */
    public JsonNode enrichObject(String objectClass, long globalId, List<String> outputFields) {
        metrics.getRequest();
        return execute(() -> restClient.get()
                .uri(b -> {
                    b.path("/api/v1/enriched-objects/{objectClass}").queryParam("globalId", globalId);
                    outputFields.forEach(f -> b.queryParam("outputField", f));
                    return b.build(objectClass);
                })
                .retrieve()
                .body(JsonNode.class));
    }

    /** Enriches a before/after pair in one request (§16). Returns the raw response (array of versions). */
    public JsonNode enrichRevisions(String objectClass, List<Long> revisionIds, List<String> outputFields) {
        metrics.revisionRequest();
        return execute(() -> restClient.post()
                .uri(b -> {
                    b.path("/api/v1/enriched-objects/{objectClass}/revisions");
                    outputFields.forEach(f -> b.queryParam("outputField", f));
                    return b.build(objectClass);
                })
                .body(revisionIds)
                .retrieve()
                .body(JsonNode.class));
    }

    private JsonNode execute(Supplier<JsonNode> httpCall) {
        EnrichException last = null;
        for (int attempt = 1; attempt <= retry.getMaxAttempts(); attempt++) {
            try {
                return protectedCall(httpCall);
            } catch (BackpressureException e) {
                throw e; // never retried / DLQ'd — redeliver under backpressure
            } catch (EnrichException e) {
                last = e;
                metrics.httpError();
                if (!e.isRetryable() || attempt == retry.getMaxAttempts()) {
                    throw e;
                }
                metrics.retry();
                sleepBackoff(attempt);
            }
        }
        throw last; // unreachable
    }

    private JsonNode protectedCall(Supplier<JsonNode> httpCall) {
        Supplier<JsonNode> decorated = Bulkhead.decorateSupplier(bulkhead,
                CircuitBreaker.decorateSupplier(circuitBreaker, () -> timedHttp(httpCall)));
        try {
            return decorated.get();
        } catch (BulkheadFullException e) {
            throw new BackpressureException("enrich bulkhead full: " + e.getMessage());
        } catch (CallNotPermittedException e) {
            throw new EnrichException(EnrichException.Kind.RETRYABLE, "enrich circuit breaker open");
        }
    }

    private JsonNode timedHttp(Supplier<JsonNode> httpCall) {
        long start = System.nanoTime();
        try {
            JsonNode body = httpCall.get();
            metrics.httpRequest("success");
            return body;
        } catch (HttpStatusCodeException e) {
            metrics.httpRequest("error");
            throw mapStatus(e);
        } catch (ResourceAccessException e) {
            // connection/read timeout, connection reset (§27 retryable)
            metrics.httpRequest("error");
            throw new EnrichException(EnrichException.Kind.RETRYABLE, "enrich I/O error: " + e.getMessage(), e);
        } finally {
            metrics.httpLatency().record(Duration.ofNanos(System.nanoTime() - start));
        }
    }

    private EnrichException mapStatus(HttpStatusCodeException e) {
        int code = e.getStatusCode().value();
        return switch (code) {
            case 429, 502, 503, 504 -> new EnrichException(EnrichException.Kind.RETRYABLE,
                    "enrich retryable HTTP " + code, e);
            case 404 -> new EnrichException(EnrichException.Kind.NOT_FOUND,
                    "enrich object/revision not found (404)", e);
            default -> new EnrichException(EnrichException.Kind.NON_RETRYABLE,
                    "enrich non-retryable HTTP " + code, e);
        };
    }

    private void sleepBackoff(int attempt) {
        double base = retry.getInitialBackoffMs() * Math.pow(retry.getMultiplier(), attempt - 1);
        double capped = Math.min(base, retry.getMaxBackoffMs());
        double jitter = capped * retry.getJitter() * ThreadLocalRandom.current().nextDouble();
        try {
            Thread.sleep((long) (capped + jitter));
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new EnrichException(EnrichException.Kind.RETRYABLE, "interrupted during backoff", ie);
        }
    }
}
