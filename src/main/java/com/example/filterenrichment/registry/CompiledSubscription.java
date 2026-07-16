package com.example.filterenrichment.registry;

import com.example.filterenrichment.filter.CompiledFilter;
import com.example.filterenrichment.metamodel.MetamodelCatalog;

import java.util.List;
import java.util.Set;

/**
 * A served subscription with its filter pre-compiled and its required output fields pre-computed.
 * Held in memory by each pod; recompiled only on load / config change.
 *
 * @param targets        the class targets (canonical name + match mode) the subscription selects
 * @param requiredFields deterministic {@code outputField} list to request from enrichment
 * @param filterFields   class-qualified fields the filter needs (to detect partial enrichment)
 * @param filter         compiled full + tri-state filter
 */
public record CompiledSubscription(
        String subscriptionId,
        String subscriberName,
        List<CompiledTarget> targets,
        List<String> requiredFields,
        Set<String> filterFields,
        CompiledFilter filter
) {

    /** A resolved target: canonical class + whether subclasses are included. */
    public record CompiledTarget(String canonical, boolean includeSubclasses) {
    }

    /**
     * Class-level candidacy: the object matches if any target selects it — for a polymorphic
     * (SUBTREE) target the target class must be an ancestor-or-self of the object's class; for an
     * exact target the classes must be equal.
     */
    public boolean matchesClass(String objectCanonical, MetamodelCatalog catalog) {
        for (CompiledTarget t : targets) {
            if (t.includeSubclasses()) {
                if (catalog.isAncestorOrSelf(t.canonical(), objectCanonical)) {
                    return true;
                }
            } else if (t.canonical().equals(objectCanonical)) {
                return true;
            }
        }
        return false;
    }
}
