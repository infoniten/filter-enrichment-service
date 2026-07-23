package com.example.filterenrichment.filter;

import com.example.filterenrichment.metamodel.MetamodelCatalog;
import com.fasterxml.jackson.databind.JsonNode;
import cz.jirutka.rsql.parser.RSQLParser;
import cz.jirutka.rsql.parser.RSQLParserException;
import cz.jirutka.rsql.parser.ast.AndNode;
import cz.jirutka.rsql.parser.ast.ComparisonNode;
import cz.jirutka.rsql.parser.ast.Node;
import cz.jirutka.rsql.parser.ast.OrNode;
import cz.jirutka.rsql.parser.ast.RSQLVisitor;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Compiles a subscription's RSQL filter once into a {@link CompiledFilter} that can be
 * evaluated both fully (on the enriched object) and as a tri-state pre-filter (on the flat payload).
 * Class-qualified filter paths are validated against the domain model; paths that reference an
 * unknown field or traverse a to-many collection fail compilation (subscription FAILED). Bare
 * flat selectors ({@code portfolioId}) are accepted as flat scalars.
 */
@Component
public class RsqlFilterCompiler {

    private static final RSQLParser PARSER = new RSQLParser();

    /**
     * @throws FilterCompileException if the filter is not valid RSQL, references an unknown field,
     *                                or traverses a collection.
     */
    public CompiledFilter compile(String filter, MetamodelCatalog catalog) {
        if (isNoFilter(filter)) {
            return CompiledFilter.ALWAYS_TRUE;
        }
        cz.jirutka.rsql.parser.ast.Node ast;
        try {
            ast = PARSER.parse(filter);
        } catch (RSQLParserException e) {
            throw new FilterCompileException("FILTER_PARSE_ERROR", "Cannot parse RSQL filter: " + e.getMessage());
        }
        return new CompiledFilter(ast.accept(new NodeVisitor(catalog)));
    }

    /**
     * "No filter configured" — matches every object of the target class. True for {@code null}, a
     * blank string, or the literal string {@code "null"} (a common serialization artifact of a null
     * value; it is not valid RSQL, so it can only mean "no filter").
     */
    private static boolean isNoFilter(String filter) {
        return filter == null || filter.isBlank() || filter.trim().equalsIgnoreCase("null");
    }

    private static final class NodeVisitor implements RSQLVisitor<CompiledFilter.Node, Void> {

        private final MetamodelCatalog catalog;

        private NodeVisitor(MetamodelCatalog catalog) {
            this.catalog = catalog;
        }

        @Override
        public CompiledFilter.Node visit(AndNode node, Void param) {
            List<CompiledFilter.Node> children = node.getChildren().stream()
                    .map(c -> c.accept(this, param)).toList();
            return new CompiledFilter.Node() {
                @Override
                public boolean full(JsonNode enriched) {
                    for (CompiledFilter.Node c : children) {
                        if (!c.full(enriched)) {
                            return false;
                        }
                    }
                    return true;
                }

                @Override
                public Tri pre(JsonNode flat) {
                    Tri result = Tri.TRUE;
                    for (CompiledFilter.Node c : children) {
                        result = result.and(c.pre(flat));
                    }
                    return result;
                }
            };
        }

        @Override
        public CompiledFilter.Node visit(OrNode node, Void param) {
            List<CompiledFilter.Node> children = node.getChildren().stream()
                    .map(c -> c.accept(this, param)).toList();
            return new CompiledFilter.Node() {
                @Override
                public boolean full(JsonNode enriched) {
                    for (CompiledFilter.Node c : children) {
                        if (c.full(enriched)) {
                            return true;
                        }
                    }
                    return false;
                }

                @Override
                public Tri pre(JsonNode flat) {
                    Tri result = Tri.FALSE;
                    for (CompiledFilter.Node c : children) {
                        result = result.or(c.pre(flat));
                    }
                    return result;
                }
            };
        }

        @Override
        public CompiledFilter.Node visit(ComparisonNode node, Void param) {
            String selector = node.getSelector();
            validate(selector);
            String[] segments = JsonPaths.jsonSegments(selector, t -> catalog.canonicalOf(t).isPresent());
            String operator = node.getOperator().getSymbol();
            List<String> args = node.getArguments();
            return new CompiledFilter.Node() {
                @Override
                public boolean full(JsonNode enriched) {
                    return Comparisons.matches(JsonPaths.readScalar(enriched, segments), operator, args);
                }

                @Override
                public Tri pre(JsonNode flat) {
                    JsonNode value = JsonPaths.readScalar(flat, segments);
                    // Absent in the flat payload -> undecidable without enrichment.
                    if (value == null) {
                        return Tri.UNKNOWN;
                    }
                    return Tri.of(Comparisons.matches(value, operator, args));
                }
            };
        }

        /** Only class-qualified selectors are validated against the model; flat selectors are accepted. */
        private void validate(String selector) {
            int dot = selector.indexOf('.');
            boolean qualified = dot > 0 && catalog.canonicalOf(selector.substring(0, dot)).isPresent();
            if (!qualified) {
                return;
            }
            switch (catalog.classifyFilterPath(selector)) {
                case TRAVERSES_COLLECTION -> throw new FilterCompileException(
                        "FILTER_TRAVERSES_COLLECTION",
                        "Filter path traverses a collection (not supported): " + selector);
                case UNKNOWN -> throw new FilterCompileException(
                        "FILTER_SCHEMA_MISMATCH",
                        "Filter references unknown field: " + selector);
                default -> {
                    // SCALAR -> ok
                }
            }
        }
    }
}
