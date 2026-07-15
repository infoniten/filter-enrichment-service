package com.example.filterenrichment.client;

import com.example.filterenrichment.config.FilterEnrichmentProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.util.Map;

/**
 * Calls the Subscription Service internal API to move a subscription to FAILED when its filter
 * cannot be compiled. Retried with exponential backoff; best-effort — the subscription is
 * already excluded from this pod's registry regardless.
 */
@Component
public class SubscriptionFailClient {

    private static final Logger log = LoggerFactory.getLogger(SubscriptionFailClient.class);

    private final RestClient restClient;
    private final RetryTemplate retryTemplate;

    public SubscriptionFailClient(RestClient.Builder builder,
                                  FilterEnrichmentProperties props,
                                  RetryTemplate retryTemplate) {
        this.restClient = builder.baseUrl(props.getSubscriptionService().getBaseUrl()).build();
        this.retryTemplate = retryTemplate;
    }

    public void fail(String subscriptionId, String reason, String message) {
        try {
            retryTemplate.execute(ctx -> {
                restClient.post()
                        .uri("/internal/subscriptions/{id}/fail", subscriptionId)
                        .body(Map.of("reason", reason, "message", message))
                        .retrieve()
                        .toBodilessEntity();
                return null;
            });
            log.info("Reported subscription {} as FAILED ({})", subscriptionId, reason);
        } catch (RestClientException e) {
            log.error("Failed to report subscription {} as FAILED: {}", subscriptionId, e.getMessage());
        }
    }
}
