package com.example.filterenrichment.registry;

import com.example.filterenrichment.domain.RuntimeSubscription;
import com.example.filterenrichment.fields.RequiredFieldsCalculator;
import com.example.filterenrichment.filter.CompiledFilter;
import com.example.filterenrichment.filter.FilterCompileException;
import com.example.filterenrichment.filter.FilterSelectors;
import com.example.filterenrichment.filter.RsqlFilterCompiler;
import com.example.filterenrichment.metamodel.MetamodelCatalog;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Compiles a {@link RuntimeSubscription} into a {@link CompiledSubscription}: resolves the class
 * targets (canonical name + match mode), computes the required output fields and compiles the RSQL
 * filter. Field applicability against the targets is validated upstream by the Subscription Service.
 */
@Component
public class SubscriptionCompiler {

    private final RsqlFilterCompiler filterCompiler;

    public SubscriptionCompiler(RsqlFilterCompiler filterCompiler) {
        this.filterCompiler = filterCompiler;
    }

    public CompiledSubscription compile(RuntimeSubscription sub, MetamodelCatalog catalog) {
        if (sub.targets() == null || sub.targets().isEmpty()) {
            throw new FilterCompileException("MISSING_TARGETS", "subscription has no targets");
        }

        List<CompiledSubscription.CompiledTarget> targets = new ArrayList<>();
        for (RuntimeSubscription.Target t : sub.targets()) {
            if (t == null || t.objectClass() == null || t.objectClass().isBlank()) {
                throw new FilterCompileException("MISSING_OBJECT_CLASS", "target has no objectClass");
            }
            // Unknown class stays raw so it simply never matches (fail-safe) rather than throwing.
            String canonical = catalog.canonicalOf(t.objectClass()).orElse(t.objectClass());
            targets.add(new CompiledSubscription.CompiledTarget(canonical, t.includeSubclasses()));
        }

        CompiledFilter filter = filterCompiler.compile(sub.filter(), catalog);
        Set<String> filterFields = FilterSelectors.extract(sub.filter());
        List<String> requiredFields = RequiredFieldsCalculator.compute(sub.fields(), sub.filter());

        return new CompiledSubscription(
                sub.subscriptionId(),
                sub.subscriberName(),
                List.copyOf(targets),
                requiredFields,
                Set.copyOf(filterFields),
                filter);
    }
}
