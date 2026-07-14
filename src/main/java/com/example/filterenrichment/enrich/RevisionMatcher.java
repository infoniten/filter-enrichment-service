package com.example.filterenrichment.enrich;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Maps a {@code POST /revisions} response back to version ids (§17). The version id is the object's
 * {@code id}. Does not rely on element order; verifies there are no duplicates and that every
 * expected id is present. Tolerates the two plausible response shapes (§37): an array of
 * {@code {id, ...payload}} objects, or an object keyed by id. Throws {@link EnrichException}
 * (NON_RETRYABLE) on any mismatch, which routes the message to the Enrichment DLQ.
 */
public final class RevisionMatcher {

    private RevisionMatcher() {
    }

    public static Map<Long, JsonNode> match(JsonNode response, List<Long> expected) {
        if (response == null || response.isNull()) {
            throw bad("empty /revisions response");
        }
        Map<Long, JsonNode> byRevision = new HashMap<>();
        if (response.isArray()) {
            for (JsonNode element : response) {
                JsonNode revNode = element.get("id");
                if (revNode == null || !revNode.canConvertToLong()) {
                    throw bad("element without a numeric id");
                }
                putUnique(byRevision, revNode.asLong(), element);
            }
        } else if (response.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> it = response.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> e = it.next();
                long rev;
                try {
                    rev = Long.parseLong(e.getKey());
                } catch (NumberFormatException nfe) {
                    throw bad("non-numeric revision key: " + e.getKey());
                }
                putUnique(byRevision, rev, e.getValue());
            }
        } else {
            throw bad("unexpected /revisions response shape");
        }

        for (Long rev : expected) {
            if (!byRevision.containsKey(rev)) {
                throw bad("missing enriched revision " + rev);
            }
        }
        return byRevision;
    }

    private static void putUnique(Map<Long, JsonNode> map, long rev, JsonNode payload) {
        if (map.put(rev, payload) != null) {
            throw bad("duplicate enriched revision " + rev);
        }
    }

    private static EnrichException bad(String message) {
        return new EnrichException(EnrichException.Kind.NON_RETRYABLE, message);
    }
}
