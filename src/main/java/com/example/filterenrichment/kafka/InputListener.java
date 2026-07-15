package com.example.filterenrichment.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Consumes the source objects topic. Records are keyed by {@code objectId} so all versions of
 * an object land on one partition and are processed in order. The offset is committed only after the
 * record is fully handled (published / dropped / DLQ'd) — at-least-once.
 */
@Component
public class InputListener {

    private final MessageProcessor processor;

    public InputListener(MessageProcessor processor) {
        this.processor = processor;
    }

    @KafkaListener(topics = "${filter-enrichment.kafka.input-topic}",
            groupId = "${filter-enrichment.kafka.consumer-group}")
    public void onRecord(ConsumerRecord<String, byte[]> record) {
        MDC.put("traceId", UUID.randomUUID().toString());
        if (record.key() != null) {
            MDC.put("objectId", record.key());
        }
        try {
            processor.process(record.key(), record.value());
        } finally {
            MDC.clear();
        }
    }
}
