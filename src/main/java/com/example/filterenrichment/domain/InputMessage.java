package com.example.filterenrichment.domain;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Parsed input message in the real source format. There is no envelope and no {@code messageType}:
 * the type is inferred from structure — a body with {@code before} and {@code after} objects is
 * {@link MessageType#BEFORE_AFTER}, a bare object is {@link MessageType#OBJECT}. The flat object
 * <em>is</em> the payload (all fields at the top level, including {@code globalId}/{@code id}).
 *
 * <p>Field mapping: {@code objectClass} &larr; {@code objectClass} (fallback {@code objectType});
 * {@code objectId} &larr; {@code globalId}; {@code revisionId} &larr; {@code id} (unique version id,
 * also the downstream dedup key).
 */
public record InputMessage(
        MessageType messageType,
        String objectClass,
        String objectId,
        Long globalId,
        Long revisionId,
        String savedAt,
        // OBJECT: the whole flat object
        JsonNode payload,
        // BEFORE_AFTER
        Version before,
        Version after
) {

    /** One version of an object; {@code revisionId} is its {@code id}, {@code payload} the flat body. */
    public record Version(Long globalId, Long revisionId, String savedAt, JsonNode payload) {
    }
}
