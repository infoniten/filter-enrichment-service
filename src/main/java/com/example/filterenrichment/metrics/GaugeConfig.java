package com.example.filterenrichment.metrics;

import com.example.filterenrichment.kafka.BackpressureManager;
import com.example.filterenrichment.registry.SubscriptionRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Configuration;

/**
 * Binds the state gauges (§35): active subscriptions, in-flight enrich requests and paused
 * partitions. Kept separate from {@link Metrics} to avoid a bean cycle (these read live component
 * state).
 */
@Configuration
public class GaugeConfig {

    public GaugeConfig(MeterRegistry meterRegistry,
                       SubscriptionRegistry registry,
                       BackpressureManager backpressure) {
        meterRegistry.gauge("filter_enrichment_active_subscriptions", registry, SubscriptionRegistry::size);
        meterRegistry.gauge("filter_enrichment_in_flight_requests", backpressure, BackpressureManager::inFlight);
        meterRegistry.gauge("filter_enrichment_paused_partitions", backpressure, BackpressureManager::pausedContainers);
    }
}
