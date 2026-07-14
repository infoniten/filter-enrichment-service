package com.example.filterenrichment.kafka;

import com.example.filterenrichment.config.FilterEnrichmentProperties;
import com.example.filterenrichment.metrics.Metrics;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes the enriched, matched result to the shared Enriched Objects topic (§25), keyed by
 * {@code objectId} so all versions of an object stay on one partition. One input record yields at
 * most one output record. Serialization failures and publish failures (after retries) go to the
 * output DLQ (§29) and the record is still acked (§30).
 */
@Component
public class OutputPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutputPublisher.class);

    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final RetryTemplate retryTemplate;
    private final ObjectMapper objectMapper;
    private final DlqPublisher dlqPublisher;
    private final Metrics metrics;
    private final String outputTopic;

    public OutputPublisher(KafkaTemplate<String, byte[]> kafkaTemplate,
                           RetryTemplate retryTemplate,
                           ObjectMapper objectMapper,
                           DlqPublisher dlqPublisher,
                           Metrics metrics,
                           FilterEnrichmentProperties props) {
        this.kafkaTemplate = kafkaTemplate;
        this.retryTemplate = retryTemplate;
        this.objectMapper = objectMapper;
        this.dlqPublisher = dlqPublisher;
        this.metrics = metrics;
        this.outputTopic = props.getKafka().getOutputTopic();
    }

    /** Publishes {@code output} keyed by {@code objectId}; routes to output DLQ on failure. */
    public void publish(String objectId, JsonNode output, byte[] originalValue) {
        byte[] payload;
        try {
            payload = objectMapper.writeValueAsBytes(output);
        } catch (Exception e) {
            log.error("Failed to serialize output for objectId={}: {}", objectId, e.getMessage());
            dlqPublisher.toOutput(objectId, originalValue, "serialization error: " + e.getMessage());
            return;
        }
        try {
            retryTemplate.execute(ctx -> kafkaTemplate.send(outputTopic, objectId, payload).get());
            metrics.output();
            log.debug("Published enriched output for objectId={} to {}", objectId, outputTopic);
        } catch (Exception e) {
            log.error("Publish failed for objectId={}: {}; routing to output DLQ", objectId, e.getMessage());
            dlqPublisher.toOutput(objectId, originalValue, "publish failed: " + e.getMessage());
        }
    }
}
