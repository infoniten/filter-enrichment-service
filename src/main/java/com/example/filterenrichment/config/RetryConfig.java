package com.example.filterenrichment.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.backoff.ExponentialRandomBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

/**
 * Shared exponential-backoff-with-jitter retry policy used by the enrich client and the
 * internal Subscription API. {@link ExponentialRandomBackOffPolicy} multiplies the interval and adds
 * random jitter on each attempt to avoid synchronized retries.
 */
@Configuration
public class RetryConfig {

    @Bean
    public RetryTemplate retryTemplate(FilterEnrichmentProperties props) {
        FilterEnrichmentProperties.Retry cfg = props.getRetry();

        ExponentialRandomBackOffPolicy backOff = new ExponentialRandomBackOffPolicy();
        backOff.setInitialInterval(cfg.getInitialBackoffMs());
        backOff.setMaxInterval(cfg.getMaxBackoffMs());
        backOff.setMultiplier(cfg.getMultiplier());

        RetryTemplate template = new RetryTemplate();
        template.setBackOffPolicy(backOff);
        template.setRetryPolicy(new SimpleRetryPolicy(cfg.getMaxAttempts()));
        return template;
    }
}
