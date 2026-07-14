package com.example.filterenrichment.metamodel;

import com.example.filterenrichment.metamodel.dto.DomainConfigResponse;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Assembles an immutable {@link MetamodelCatalog} from the Enrich Service domain config (§7).
 */
public final class MetamodelCatalogFactory {

    private MetamodelCatalogFactory() {
    }

    public static MetamodelCatalog build(DomainConfigResponse domain) {
        Map<String, String> sourceValueToCanonical = new HashMap<>();
        if (domain.classes() != null) {
            for (DomainConfigResponse.ClassEntry c : domain.classes()) {
                if (c.name() != null && c.sourceValue() != null) {
                    sourceValueToCanonical.put(c.sourceValue(), c.name());
                }
            }
        }

        Map<String, Set<String>> scalarFieldsByCanonical = new HashMap<>();
        if (domain.fields() != null) {
            for (Map.Entry<String, DomainConfigResponse.FieldsBlock> e : domain.fields().entrySet()) {
                Set<String> names = new LinkedHashSet<>();
                DomainConfigResponse.FieldsBlock block = e.getValue();
                if (block != null && block.declaredFields() != null) {
                    for (DomainConfigResponse.FieldEntry f : block.declaredFields()) {
                        if (f.name() != null) {
                            names.add(f.name());
                        }
                    }
                }
                scalarFieldsByCanonical.put(e.getKey(), names);
            }
        }

        Map<String, List<String>> parentsOrSelf = new HashMap<>();
        if (domain.hierarchy() != null && domain.hierarchy().parentsOrSelf() != null) {
            parentsOrSelf.putAll(domain.hierarchy().parentsOrSelf());
        }

        Map<String, Map<String, MetamodelCatalog.Relation>> relationsByCanonical = new HashMap<>();
        if (domain.relations() != null) {
            for (Map.Entry<String, List<DomainConfigResponse.RelationEntry>> e : domain.relations().entrySet()) {
                if (e.getValue() == null) {
                    continue;
                }
                Map<String, MetamodelCatalog.Relation> byAlias = new HashMap<>();
                for (DomainConfigResponse.RelationEntry r : e.getValue()) {
                    String alias = r.pathName();
                    if (alias != null && r.targetClass() != null) {
                        byAlias.put(alias, new MetamodelCatalog.Relation(r.targetClass(), r.isCollection()));
                    }
                }
                if (!byAlias.isEmpty()) {
                    relationsByCanonical.put(e.getKey(), byAlias);
                }
            }
        }

        return new MetamodelCatalog(
                Map.copyOf(sourceValueToCanonical),
                Map.copyOf(scalarFieldsByCanonical),
                Map.copyOf(parentsOrSelf),
                Map.copyOf(relationsByCanonical));
    }
}
