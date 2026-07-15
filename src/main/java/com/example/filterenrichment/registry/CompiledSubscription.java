package com.example.filterenrichment.registry;

import com.example.filterenrichment.filter.CompiledFilter;
import com.example.filterenrichment.metamodel.MetamodelCatalog;

import java.util.List;
import java.util.Set;

/**
 * A served subscription with its filter pre-compiled and its required output fields pre-computed
 *. Held in memory by each pod; recompiled only on load / config change.
 *
 * @param objectCanonical      canonical class the subscription targets
 * @param referencedCanonicals canonical classes referenced by fields + filter (for candidacy)
 * @param requiredFields       deterministic {@code outputField} list to request from enrichment
 * @param filterFields         class-qualified fields the filter needs (to detect partial enrichment)
 * @param filter               compiled full + tri-state filter
 */
public record CompiledSubscription(
        String subscriptionId,
        String subscriberName,
        String objectCanonical,
        Set<String> referencedCanonicals,
        List<String> requiredFields,
        Set<String> filterFields,
        CompiledFilter filter
) {

    /**
     * Class-level candidacy: the object's class must be the subscription's class or a subtype,
     * and every referenced class must be an ancestor-or-self of the object's class.
     */
    public boolean matchesClass(String objectCanonical, MetamodelCatalog catalog) {
        return catalog.isCandidate(objectCanonical, referencedCanonicals);
    }
}
