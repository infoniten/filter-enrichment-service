package com.example.filterenrichment.kafka;

import com.example.filterenrichment.config.FilterEnrichmentProperties;
import com.example.filterenrichment.metrics.Metrics;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Routes unprocessable records to the three dead-letter queues: input (bad message), enrichment
 * (enrich failed / revision missing / filter not computable) and output (serialize / publish failed).
 * The original bytes are preserved with an {@code error-reason} header. A DLQ write is retried; if it
 * ultimately fails the exception propagates so the record is redelivered (at-least-once).
 */
@Component
public class DlqPublisher {

    private static final Logger log = LoggerFactory.getLogger(DlqPublisher.class);

    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final RetryTemplate retryTemplate;
    private final FilterEnrichmentProperties.Kafka.Dlq dlq;
    private final Metrics metrics;

    public DlqPublisher(KafkaTemplate<String, byte[]> kafkaTemplate,
                        RetryTemplate retryTemplate,
                        FilterEnrichmentProperties props,
                        Metrics metrics) {
        this.kafkaTemplate = kafkaTemplate;
        this.retryTemplate = retryTemplate;
        this.dlq = props.getKafka().getDlq();
        this.metrics = metrics;
    }

    public void toInput(String key, byte[] value, String reason) {
        send(dlq.getInput(), key, value, reason);
        metrics.dlq("input");
    }

    public void toEnrichment(String key, byte[] value, String reason) {
        send(dlq.getEnrichment(), key, value, reason);
        metrics.dlq("enrichment");
    }

    public void toOutput(String key, byte[] value, String reason) {
        send(dlq.getOutput(), key, value, reason);
        metrics.dlq("output");
    }

    private void send(String topic, String key, byte[] value, String reason) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, value);
        record.headers().add(new RecordHeader("error-reason",
                reason == null ? new byte[0] : reason.getBytes(StandardCharsets.UTF_8)));
        try {
            retryTemplate.execute(ctx -> kafkaTemplate.send(record).get());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted publishing to DLQ " + topic, e);
        } catch (Exception e) {
            // Let it propagate: the input offset is not committed and the record is redelivered.
            throw new IllegalStateException("failed to publish to DLQ " + topic, e);
        }
        log.warn("Routed record (key={}) to DLQ {}: {}", key, topic, reason);
    }
}
