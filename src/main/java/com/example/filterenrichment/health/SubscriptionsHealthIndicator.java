package com.example.filterenrichment.health;

import com.example.filterenrichment.registry.RuntimeConfigService;
import com.example.filterenrichment.registry.SubscriptionRegistry;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/** Readiness contributor {@code subscriptions} (§34): UP once the runtime registry is loaded. */
@Component("subscriptions")
public class SubscriptionsHealthIndicator implements HealthIndicator {

    private final RuntimeConfigService runtimeConfigService;
    private final SubscriptionRegistry registry;

    public SubscriptionsHealthIndicator(RuntimeConfigService runtimeConfigService,
                                        SubscriptionRegistry registry) {
        this.runtimeConfigService = runtimeConfigService;
        this.registry = registry;
    }

    @Override
    public Health health() {
        if (!runtimeConfigService.isLoaded()) {
            return Health.down().withDetail("reason", "runtime subscriptions not loaded").build();
        }
        return Health.up().withDetail("served", registry.size()).build();
    }
}
