package com.example.filterenrichment.metamodel;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Immutable, resolved view of the domain model used to:
 * <ul>
 *   <li>normalize an {@code objectClass} token to its canonical name;</li>
 *   <li>decide candidate subscriptions (a subscription defined on class S matches an object of
 *       class X iff S is an ancestor-or-self of X, so subclasses match superclass subscriptions);</li>
 *   <li>classify a filter path so paths traversing to-many collections are rejected at compile
 *       time (filters are supported only over scalar / to-one paths).</li>
 * </ul>
 * All maps are keyed by canonical class name (UPPER_SNAKE); lookups walk {@code parentsOrSelf} so
 * inherited members resolve correctly.
 */
public final class MetamodelCatalog {

    public enum PathKind {
        /** Valid path ending in a scalar field (safe for filter). */
        SCALAR,
        /** Path traverses a to-many collection (not supported in filters). */
        TRAVERSES_COLLECTION,
        /** Unknown class/field/relation, or a non-scalar leaf. */
        UNKNOWN
    }

    /** A relation edge: canonical target class + whether it is a to-many collection. */
    public record Relation(String target, boolean collection) {
    }

    private final Map<String, String> sourceValueToCanonical;
    private final Map<String, Set<String>> scalarFieldsByCanonical;
    private final Map<String, List<String>> parentsOrSelf;
    private final Map<String, Map<String, Relation>> relationsByCanonical;

    MetamodelCatalog(Map<String, String> sourceValueToCanonical,
                     Map<String, Set<String>> scalarFieldsByCanonical,
                     Map<String, List<String>> parentsOrSelf,
                     Map<String, Map<String, Relation>> relationsByCanonical) {
        this.sourceValueToCanonical = sourceValueToCanonical;
        this.scalarFieldsByCanonical = scalarFieldsByCanonical;
        this.parentsOrSelf = parentsOrSelf;
        this.relationsByCanonical = relationsByCanonical;
    }

    public int classCount() {
        return sourceValueToCanonical.size();
    }

    /** Resolves a class token (sourceValue, or already-canonical) to canonical, if known. */
    public Optional<String> canonicalOf(String token) {
        if (token == null) {
            return Optional.empty();
        }
        String canonical = sourceValueToCanonical.get(token);
        if (canonical != null) {
            return Optional.of(canonical);
        }
        return scalarFieldsByCanonical.containsKey(token) ? Optional.of(token) : Optional.empty();
    }

    /** Canonical class referenced by a field path's first (class) segment. */
    public Optional<String> referencedClass(String rawPath) {
        int dot = rawPath.indexOf('.');
        if (dot <= 0) {
            return Optional.empty();
        }
        return canonicalOf(rawPath.substring(0, dot));
    }

    /**
     * Candidacy: true iff every referenced canonical class is an ancestor-or-self of the object's
     * class (so an object of a subclass matches a subscription defined on a superclass).
     */
    public boolean isCandidate(String objectCanonical, Collection<String> referencedCanonicals) {
        return chain(objectCanonical).containsAll(referencedCanonicals);
    }

    /** True if {@code ancestorCanonical} is {@code objectCanonical} or one of its ancestors. */
    public boolean isAncestorOrSelf(String ancestorCanonical, String objectCanonical) {
        return chain(objectCanonical).contains(ancestorCanonical);
    }

    /**
     * Classifies a filter path (used at compile time). Filters are supported only for
     * {@link PathKind#SCALAR}; {@link PathKind#TRAVERSES_COLLECTION} and {@link PathKind#UNKNOWN}
     * fail the subscription.
     */
    public PathKind classifyFilterPath(String rawPath) {
        String[] parts = rawPath.split("\\.");
        if (parts.length < 2) {
            return PathKind.UNKNOWN;
        }
        Optional<String> canonical = canonicalOf(parts[0]);
        if (canonical.isEmpty()) {
            return PathKind.UNKNOWN;
        }
        String current = canonical.get();
        for (int i = 1; i < parts.length; i++) {
            String seg = parts[i];
            boolean last = i == parts.length - 1;
            if (last) {
                return hasScalarField(current, seg) ? PathKind.SCALAR : PathKind.UNKNOWN;
            }
            Relation rel = relation(current, seg);
            if (rel == null) {
                return PathKind.UNKNOWN;
            }
            if (rel.collection()) {
                return PathKind.TRAVERSES_COLLECTION;
            }
            current = rel.target();
        }
        return PathKind.UNKNOWN;
    }

    private boolean hasScalarField(String canonical, String field) {
        for (String ancestor : chain(canonical)) {
            Set<String> fields = scalarFieldsByCanonical.get(ancestor);
            if (fields != null && fields.contains(field)) {
                return true;
            }
        }
        return false;
    }

    private Relation relation(String canonical, String alias) {
        for (String ancestor : chain(canonical)) {
            Map<String, Relation> rels = relationsByCanonical.get(ancestor);
            if (rels != null && rels.containsKey(alias)) {
                return rels.get(alias);
            }
        }
        return null;
    }

    private List<String> chain(String canonical) {
        List<String> chain = parentsOrSelf.get(canonical);
        return chain != null && !chain.isEmpty() ? chain : List.of(canonical);
    }
}
