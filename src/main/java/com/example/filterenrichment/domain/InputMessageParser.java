package com.example.filterenrichment.domain;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

/**
 * Parses the real source message format into an {@link InputMessage} (§9/§10/§13). The type is
 * inferred from structure (before+after vs a bare object); the flat object is the payload. Any
 * malformed body or missing mandatory field raises {@link InputParseException}, which the processor
 * routes to the input DLQ.
 */
@Component
public class InputMessageParser {

    private final ObjectMapper mapper;

    public InputMessageParser(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public InputMessage parse(byte[] value) {
        JsonNode root;
        try {
            root = mapper.readTree(value);
        } catch (Exception e) {
            throw new InputParseException("invalid JSON: " + e.getMessage(), e);
        }
        if (root == null || !root.isObject()) {
            throw new InputParseException("message is not a JSON object");
        }

        boolean hasBefore = isObject(root.get("before"));
        boolean hasAfter = isObject(root.get("after"));
        if (hasBefore || hasAfter) {
            if (!hasBefore || !hasAfter) {
                throw new InputParseException("BEFORE_AFTER requires both before and after objects");
            }
            return beforeAfter(root.get("before"), root.get("after"));
        }
        return object(root);
    }

    private InputMessage object(JsonNode node) {
        String objectClass = requireClass(node);
        Long globalId = requireLong(node, "globalId");
        Long id = requireLong(node, "id");
        return new InputMessage(
                MessageType.OBJECT,
                objectClass,
                String.valueOf(globalId),
                globalId,
                id,
                text(node, "savedAt"),
                node,
                null,
                null);
    }

    private InputMessage beforeAfter(JsonNode beforeNode, JsonNode afterNode) {
        InputMessage.Version before = version(beforeNode, "before");
        InputMessage.Version after = version(afterNode, "after");
        String objectClass = requireClass(afterNode);
        return new InputMessage(
                MessageType.BEFORE_AFTER,
                objectClass,
                String.valueOf(after.globalId()),
                after.globalId(),
                after.revisionId(),
                after.savedAt(),
                null,
                before,
                after);
    }

    private InputMessage.Version version(JsonNode node, String tag) {
        if (!isObject(node)) {
            throw new InputParseException("missing " + tag + " object");
        }
        requireClass(node);
        Long globalId = requireLong(node, "globalId");
        Long id = requireLong(node, "id"); // unique version id -> revisionId
        return new InputMessage.Version(globalId, id, text(node, "savedAt"), node);
    }

    /** objectClass with fallback to the (soon-to-be-renamed) objectType. */
    private String requireClass(JsonNode node) {
        String cls = text(node, "objectClass");
        if (cls == null) {
            cls = text(node, "objectType");
        }
        if (cls == null || cls.isBlank()) {
            throw new InputParseException("missing objectClass/objectType");
        }
        return cls;
    }

    private Long requireLong(JsonNode node, String field) {
        JsonNode v = node.get(field);
        if (v == null || v.isNull() || !v.canConvertToLong()) {
            throw new InputParseException("missing/invalid " + field);
        }
        return v.asLong();
    }

    private String text(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return v == null || v.isNull() ? null : v.asText();
    }

    private boolean isObject(JsonNode node) {
        return node != null && node.isObject();
    }
}
