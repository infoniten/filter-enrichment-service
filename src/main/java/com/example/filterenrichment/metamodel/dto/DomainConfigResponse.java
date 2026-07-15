package com.example.filterenrichment.metamodel.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;

/**
 * Subset of the Object Enrich Service {@code GET /api/config/domain} response needed here:
 * classes (sourceValue &lt;-&gt; canonical), declared scalar fields per class, the class hierarchy,
 * and relations (with type, to detect to-many collections that filters may not traverse).
 *
 * <p>Unknown properties are ignored so the contract can evolve; the exact response shape is pinned
 * by a contract test.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record DomainConfigResponse(
        List<ClassEntry> classes,
        Map<String, FieldsBlock> fields,
        Hierarchy hierarchy,
        Map<String, List<RelationEntry>> relations
) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ClassEntry(String name, String sourceValue) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record FieldsBlock(List<FieldEntry> declaredFields) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record FieldEntry(String name, String type) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Hierarchy(Map<String, List<String>> parentsOrSelf) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record RelationEntry(String name, String alias, String type, String targetClass) {

        public String pathName() {
            return alias != null && !alias.isBlank() ? alias : name;
        }

        /** to-many relations (EMBEDDED_SET / GLOBAL_SET) resolve to collections. */
        public boolean isCollection() {
            return type != null && type.toUpperCase().contains("SET");
        }
    }
}
