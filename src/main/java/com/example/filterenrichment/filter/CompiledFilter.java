package com.example.filterenrichment.filter;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * A subscription filter compiled once (§8) into two evaluators over a JSON payload:
 * <ul>
 *   <li>{@link #matches(JsonNode)} — the full boolean filter, evaluated on the <em>enriched</em>
 *       object (§11/§18);</li>
 *   <li>{@link #preMatch(JsonNode)} — the Kleene tri-state pre-filter, evaluated on the <em>flat</em>
 *       input payload to cheaply exclude non-candidates without enrichment (§11/§15).</li>
 * </ul>
 */
public final class CompiledFilter {

    /** A compiled filter node exposing both the full and the tri-state evaluation. */
    public interface Node {
        boolean full(JsonNode enriched);

        Tri pre(JsonNode flat);
    }

    /** Filter that always matches (no filter configured). */
    public static final CompiledFilter ALWAYS_TRUE = new CompiledFilter(new Node() {
        @Override
        public boolean full(JsonNode enriched) {
            return true;
        }

        @Override
        public Tri pre(JsonNode flat) {
            return Tri.TRUE;
        }
    });

    private final Node root;

    CompiledFilter(Node root) {
        this.root = root;
    }

    public boolean matches(JsonNode enriched) {
        return root.full(enriched);
    }

    public Tri preMatch(JsonNode flatPayload) {
        return root.pre(flatPayload);
    }
}
