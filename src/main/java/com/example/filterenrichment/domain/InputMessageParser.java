package com.example.filterenrichment.domain;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

/**
 * Parses the real source message format into an {@link InputMessage}. The type is
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
        if (hasBefore && hasAfter) {
            return beforeAfter(root.get("before"), root.get("after"));
        }
        if (hasAfter) {
            // First version of the object: `before` is null/absent, so there is no prior revision to
            // compare against — the object can only enter the selection, never leave it. Handle it as
            // a plain OBJECT on the `after` body (same downstream effect: delivered iff it matches).
            return object(root.get("after"));
        }
        if (hasBefore) {
            // Only `before` present (`after` null/absent): a deletion — not a supported change shape.
            throw new InputParseException("BEFORE_AFTER requires an 'after' object");
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
        if (v != null && !v.isNull()) {
            // Numeric node (long/int).
            if (v.canConvertToLong()) {
                return v.asLong();
            }
            // Numeric value sent as a JSON string, e.g. "globalId": "110831655".
            if (v.isTextual()) {
                String s = v.asText().trim();
                if (!s.isEmpty()) {
                    try {
                        return Long.parseLong(s);
                    } catch (NumberFormatException ignored) {
                        // fall through to the error below
                    }
                }
            }
        }
        throw new InputParseException("missing/invalid " + field);
    }

    private String text(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return v == null || v.isNull() ? null : v.asText();
    }

    private boolean isObject(JsonNode node) {
        return node != null && node.isObject();
    }
}
