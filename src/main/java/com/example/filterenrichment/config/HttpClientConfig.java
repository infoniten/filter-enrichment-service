package com.example.filterenrichment.config;

import com.example.filterenrichment.enrich.EnrichException;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.core5.util.Timeout;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.time.Duration;

/**
 * Object Enrich Service HTTP client: connection pooling + keep-alive, configurable
 * connect/read timeouts and pool size, plus a shared circuit breaker and bulkhead used
 * programmatically by the enrich client to bound concurrency and fail fast when the upstream is down.
 */
@Configuration
public class HttpClientConfig {

    /**
     * RestClient pointed at DataDictionary, used only to fetch the metamodel at startup / on reloads
     * (not per message). Backed by a small pooled Apache HttpClient with its own timeouts.
     */
    @Bean("metamodelRestClient")
    public RestClient metamodelRestClient(RestClient.Builder builder, FilterEnrichmentProperties props) {
        FilterEnrichmentProperties.Metamodel cfg = props.getMetamodel();

        ConnectionConfig connectionConfig = ConnectionConfig.custom()
                .setConnectTimeout(Timeout.ofMilliseconds(cfg.getConnectTimeoutMs()))
                .setSocketTimeout(Timeout.ofMilliseconds(cfg.getReadTimeoutMs()))
                .build();

        PoolingHttpClientConnectionManager connectionManager = PoolingHttpClientConnectionManagerBuilder.create()
                .setDefaultConnectionConfig(connectionConfig)
                .build();

        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionManager(connectionManager)
                .build();

        HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory(httpClient);

        return builder.baseUrl(cfg.getBaseUrl()).requestFactory(factory).build();
    }

    /** RestClient pointed at the Enrich Service, backed by a pooled Apache HttpClient. */
    @Bean("enrichRestClient")
    public RestClient enrichRestClient(RestClient.Builder builder, FilterEnrichmentProperties props) {
        FilterEnrichmentProperties.Enrich cfg = props.getEnrich();

        ConnectionConfig connectionConfig = ConnectionConfig.custom()
                .setConnectTimeout(Timeout.ofMilliseconds(cfg.getConnectTimeoutMs()))
                .setSocketTimeout(Timeout.ofMilliseconds(cfg.getReadTimeoutMs()))
                .build();

        PoolingHttpClientConnectionManager connectionManager = PoolingHttpClientConnectionManagerBuilder.create()
                .setMaxConnTotal(cfg.getMaxConnections())
                .setMaxConnPerRoute(cfg.getMaxConnectionsPerRoute())
                .setDefaultConnectionConfig(connectionConfig)
                .build();

        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionManager(connectionManager)
                .build();

        // Connect/read timeouts are applied by the pooled connection manager (ConnectionConfig above).
        HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory(httpClient);

        return builder.baseUrl(cfg.getBaseUrl()).requestFactory(factory).build();
    }

    @Bean("enrichCircuitBreaker")
    public CircuitBreaker enrichCircuitBreaker(FilterEnrichmentProperties props) {
        FilterEnrichmentProperties.Enrich.CircuitBreaker cb = props.getEnrich().getCircuitBreaker();
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .failureRateThreshold(cb.getFailureRateThreshold())
                .waitDurationInOpenState(Duration.ofMillis(cb.getWaitDurationInOpenStateMs()))
                .slidingWindowSize(cb.getSlidingWindowSize())
                .minimumNumberOfCalls(cb.getMinimumNumberOfCalls())
                // Only upstream-health failures trip the breaker; client errors (400/404) do not.
                .recordException(e -> e instanceof EnrichException ee && ee.isRetryable())
                .build();
        return CircuitBreaker.of("enrich", config);
    }

    @Bean("enrichBulkhead")
    public Bulkhead enrichBulkhead(FilterEnrichmentProperties props) {
        FilterEnrichmentProperties.Backpressure bp = props.getBackpressure();
        BulkheadConfig config = BulkheadConfig.custom()
                .maxConcurrentCalls(bp.getMaxConcurrentHttpRequests())
                .maxWaitDuration(Duration.ofMillis(bp.getAcquireTimeoutMs()))
                .build();
        return Bulkhead.of("enrich", config);
    }
}
