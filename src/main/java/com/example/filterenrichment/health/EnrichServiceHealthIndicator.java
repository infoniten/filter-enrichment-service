package com.example.filterenrichment.health;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Readiness contributor {@code enrichService}: DOWN only while the circuit breaker is OPEN
 * (upstream considered unavailable); UP otherwise, including HALF_OPEN (probing).
 */
@Component("enrichService")
public class EnrichServiceHealthIndicator implements HealthIndicator {

    private final CircuitBreaker circuitBreaker;

    public EnrichServiceHealthIndicator(@Qualifier("enrichCircuitBreaker") CircuitBreaker circuitBreaker) {
        this.circuitBreaker = circuitBreaker;
    }

    @Override
    public Health health() {
        CircuitBreaker.State state = circuitBreaker.getState();
        if (state == CircuitBreaker.State.OPEN || state == CircuitBreaker.State.FORCED_OPEN) {
            return Health.down().withDetail("circuitBreaker", state.name()).build();
        }
        return Health.up().withDetail("circuitBreaker", state.name()).build();
    }
}
