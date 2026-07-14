package com.example.filterenrichment.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Iterator;
import java.util.Map;

/**
 * Deep-merges an enriched payload over the flat source payload so the combined object carries both
 * the source-flat fields and the enriched (nested) fields for filtering and output (§21/§22).
 */
public final class JsonMerge {

    private JsonMerge() {
    }

    /**
     * Returns a new node: a deep copy of {@code base} with {@code overlay} merged in (overlay wins on
     * scalar conflicts; objects merge recursively). Non-object inputs fall back to overlay-or-base.
     */
    public static JsonNode deepMerge(JsonNode base, JsonNode overlay) {
        if (base == null || !base.isObject()) {
            return overlay != null ? overlay : base;
        }
        ObjectNode result = base.deepCopy();
        if (overlay == null || !overlay.isObject()) {
            return result;
        }
        Iterator<Map.Entry<String, JsonNode>> it = overlay.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = it.next();
            JsonNode existing = result.get(e.getKey());
            if (existing != null && existing.isObject() && e.getValue().isObject()) {
                result.set(e.getKey(), deepMerge(existing, e.getValue()));
            } else {
                result.set(e.getKey(), e.getValue());
            }
        }
        return result;
    }
}
