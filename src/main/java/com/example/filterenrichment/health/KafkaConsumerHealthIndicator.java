package com.example.filterenrichment.health;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

/** Readiness contributor {@code kafkaConsumer}: UP when the listener containers are running. */
@Component("kafkaConsumer")
public class KafkaConsumerHealthIndicator implements HealthIndicator {

    private final KafkaListenerEndpointRegistry registry;

    public KafkaConsumerHealthIndicator(KafkaListenerEndpointRegistry registry) {
        this.registry = registry;
    }

    @Override
    public Health health() {
        var containers = registry.getListenerContainers();
        if (containers.isEmpty()) {
            return Health.down().withDetail("reason", "no listener containers").build();
        }
        for (MessageListenerContainer c : containers) {
            if (!c.isRunning()) {
                return Health.down().withDetail("reason", "listener container not running").build();
            }
        }
        return Health.up().withDetail("containers", containers.size()).build();
    }
}
