package com.example.filterenrichment.filter;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Lexically extracts class-qualified selectors (e.g. {@code Trade.counterparty.code}) from an RSQL
 * filter. These are the filter fields that must be enrichment-fetched; bare flat selectors
 * (e.g. {@code portfolioId}) are already present in the source payload and are not extracted.
 */
public final class FilterSelectors {

    private static final Pattern SELECTOR =
            Pattern.compile("[A-Z][A-Za-z0-9]*(?:\\.[A-Za-z0-9_]+)+");

    private FilterSelectors() {
    }

    public static Set<String> extract(String filter) {
        Set<String> selectors = new LinkedHashSet<>();
        if (filter == null || filter.isBlank()) {
            return selectors;
        }
        Matcher m = SELECTOR.matcher(filter);
        while (m.find()) {
            selectors.add(m.group());
        }
        return selectors;
    }
}
