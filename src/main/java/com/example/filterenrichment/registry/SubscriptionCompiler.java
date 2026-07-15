package com.example.filterenrichment.registry;

import com.example.filterenrichment.domain.RuntimeSubscription;
import com.example.filterenrichment.fields.RequiredFieldsCalculator;
import com.example.filterenrichment.filter.CompiledFilter;
import com.example.filterenrichment.filter.FilterSelectors;
import com.example.filterenrichment.filter.RsqlFilterCompiler;
import com.example.filterenrichment.metamodel.MetamodelCatalog;
import org.springframework.stereotype.Component;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Compiles a {@link RuntimeSubscription} into a {@link CompiledSubscription}: resolves the target
 * class, computes the referenced classes (for candidacy), the required output fields and the
 * compiled RSQL filter.
 */
@Component
public class SubscriptionCompiler {

    private final RsqlFilterCompiler filterCompiler;

    public SubscriptionCompiler(RsqlFilterCompiler filterCompiler) {
        this.filterCompiler = filterCompiler;
    }

    public CompiledSubscription compile(RuntimeSubscription sub, MetamodelCatalog catalog) {
        CompiledFilter filter = filterCompiler.compile(sub.filter(), catalog);

        String objectCanonical = catalog.canonicalOf(sub.objectClass()).orElse(sub.objectClass());

        Set<String> filterFields = FilterSelectors.extract(sub.filter());

        Set<String> referenced = new LinkedHashSet<>();
        referenced.add(objectCanonical);
        if (sub.fields() != null) {
            for (String field : sub.fields()) {
                addReferenced(referenced, field, catalog);
            }
        }
        for (String selector : filterFields) {
            addReferenced(referenced, selector, catalog);
        }

        List<String> requiredFields = RequiredFieldsCalculator.compute(sub.fields(), sub.filter());

        return new CompiledSubscription(
                sub.subscriptionId(),
                sub.subscriberName(),
                objectCanonical,
                Set.copyOf(referenced),
                requiredFields,
                Set.copyOf(filterFields),
                filter);
    }

    private void addReferenced(Set<String> referenced, String path, MetamodelCatalog catalog) {
        int dot = path.indexOf('.');
        String prefix = dot > 0 ? path.substring(0, dot) : path;
        // Unknown prefixes are kept raw so candidacy fails safe (the subscription won't spuriously match).
        referenced.add(catalog.canonicalOf(prefix).orElse(prefix));
    }
}
