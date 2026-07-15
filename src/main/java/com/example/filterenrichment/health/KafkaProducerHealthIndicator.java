package com.example.filterenrichment.health;

import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/** Readiness contributor {@code kafkaProducer}: UP when the Kafka cluster is reachable. */
@Component("kafkaProducer")
public class KafkaProducerHealthIndicator implements HealthIndicator {

    private final AdminClient adminClient;

    public KafkaProducerHealthIndicator(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    @Override
    public Health health() {
        try {
            int brokers = adminClient.describeCluster().nodes().get(2, TimeUnit.SECONDS).size();
            return Health.up().withDetail("brokers", brokers).build();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Health.down().withDetail("reason", "interrupted").build();
        } catch (Exception e) {
            return Health.down().withDetail("reason", "cluster unreachable: " + e.getMessage()).build();
        }
    }
}
