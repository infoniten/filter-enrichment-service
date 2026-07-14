package com.example.filterenrichment.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Parsed input Kafka message (§10/§13). One record models both formats: {@code OBJECT} carries a
 * single object (top-level {@code globalId}/{@code revisionId}/{@code payload}); {@code BEFORE_AFTER}
 * carries {@link #before()} and {@link #after()} versions. Absent/optional fields are validated per
 * type by {@code InputMessageValidator}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record InputMessage(
        MessageType messageType,
        String sourceEventId,
        String objectClass,
        String objectId,
        String savedAt,
        // OBJECT
        Long globalId,
        Long revisionId,
        JsonNode payload,
        // BEFORE_AFTER
        Version before,
        Version after
) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Version(Long globalId, Long revisionId, JsonNode payload) {
    }
}
