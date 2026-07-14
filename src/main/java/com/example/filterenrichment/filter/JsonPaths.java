package com.example.filterenrichment.filter;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.function.Predicate;

/**
 * Field-path navigation over a (flat or enriched) object payload. A selector is either a bare flat
 * field ({@code portfolioId}) or class-qualified ({@code Trade.counterparty.code}); when the leading
 * segment is a known class it is metamodel context and is stripped, and navigation follows the
 * remaining json-name segments from the root.
 */
public final class JsonPaths {

    private JsonPaths() {
    }

    /**
     * JSON navigation segments for a selector: strips a leading class segment when {@code isKnownClass}
     * accepts it, otherwise treats every dotted segment as a json path (so bare flat fields work).
     */
    public static String[] jsonSegments(String selector, Predicate<String> isKnownClass) {
        int dot = selector.indexOf('.');
        if (dot > 0 && isKnownClass.test(selector.substring(0, dot))) {
            return selector.substring(dot + 1).split("\\.");
        }
        return selector.split("\\.");
    }

    /**
     * Reads a scalar value at a flat (non-collection) path. Returns {@code null} when any segment is
     * absent or a non-object is encountered mid-path.
     */
    public static JsonNode readScalar(JsonNode root, String[] segments) {
        JsonNode current = root;
        for (String seg : segments) {
            if (current == null || !current.isObject()) {
                return null;
            }
            current = current.get(seg);
            if (current == null || current.isMissingNode()) {
                return null;
            }
        }
        return current;
    }
}
