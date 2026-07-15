package com.example.filterenrichment.fields;

import com.example.filterenrichment.filter.FilterSelectors;

import java.util.List;
import java.util.TreeSet;

/**
 * Computes the {@code outputField} set required to serve a subscription:
 * {@code union(subscription.fields) + union(class-qualified fields referenced by the filter)}, with
 * duplicates removed and a deterministic (lexicographic) order. A field used only by the filter is
 * still fetched so the filter can be evaluated on enriched data.
 */
public final class RequiredFieldsCalculator {

    private RequiredFieldsCalculator() {
    }

    public static List<String> compute(List<String> subscriptionFields, String filter) {
        TreeSet<String> fields = new TreeSet<>();
        if (subscriptionFields != null) {
            for (String f : subscriptionFields) {
                if (f != null && !f.isBlank()) {
                    fields.add(f.trim());
                }
            }
        }
        fields.addAll(FilterSelectors.extract(filter));
        return List.copyOf(fields);
    }
}
