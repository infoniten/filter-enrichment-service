package com.example.filterenrichment.kafka;

import com.example.filterenrichment.domain.EnrichmentStatus;
import com.example.filterenrichment.domain.InputMessage;
import com.example.filterenrichment.domain.InputMessageValidator;
import com.example.filterenrichment.enrich.EnrichClient;
import com.example.filterenrichment.enrich.EnrichException;
import com.example.filterenrichment.enrich.RevisionMatcher;
import com.example.filterenrichment.filter.JsonPaths;
import com.example.filterenrichment.filter.Tri;
import com.example.filterenrichment.metamodel.MetamodelCatalog;
import com.example.filterenrichment.metamodel.MetamodelHolder;
import com.example.filterenrichment.metrics.Metrics;
import com.example.filterenrichment.registry.CompiledSubscription;
import com.example.filterenrichment.registry.SubscriptionRegistry;
import com.example.filterenrichment.util.JsonMerge;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;

/**
 * Core pipeline (§11/§14/§18): parse & validate the input record, pre-match candidate subscriptions
 * on the flat payload, enrich once via the Object Enrich Service, apply full filters on the enriched
 * data, and publish a single matched output record — or drop / DLQ. Never treats a filter as false
 * merely because a field is missing (§28): if a filter cannot be computed the message goes to the
 * Enrichment DLQ.
 */
@Component
public class MessageProcessor {

    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);

    private final ObjectMapper mapper;
    private final SubscriptionRegistry registry;
    private final MetamodelHolder metamodel;
    private final EnrichClient enrichClient;
    private final BackpressureManager backpressure;
    private final OutputPublisher outputPublisher;
    private final DlqPublisher dlqPublisher;
    private final Metrics metrics;

    public MessageProcessor(ObjectMapper mapper,
                            SubscriptionRegistry registry,
                            MetamodelHolder metamodel,
                            EnrichClient enrichClient,
                            BackpressureManager backpressure,
                            OutputPublisher outputPublisher,
                            DlqPublisher dlqPublisher,
                            Metrics metrics) {
        this.mapper = mapper;
        this.registry = registry;
        this.metamodel = metamodel;
        this.enrichClient = enrichClient;
        this.backpressure = backpressure;
        this.outputPublisher = outputPublisher;
        this.dlqPublisher = dlqPublisher;
        this.metrics = metrics;
    }

    public void process(String key, byte[] value) {
        metrics.inputReceived();

        InputMessage msg;
        try {
            msg = mapper.readValue(value, InputMessage.class);
        } catch (Exception e) {
            dlqPublisher.toInput(key, value, "malformed message: " + e.getMessage());
            return;
        }

        var invalid = InputMessageValidator.validate(msg);
        if (invalid.isPresent()) {
            dlqPublisher.toInput(key, value, invalid.get());
            return;
        }
        metrics.inputType(msg.messageType());
        MDC.put("sourceEventId", String.valueOf(msg.sourceEventId()));
        MDC.put("messageType", String.valueOf(msg.messageType()));
        MDC.put("objectClass", String.valueOf(msg.objectClass()));

        MetamodelCatalog catalog = metamodel.get(); // throws if not loaded -> record redelivered
        String objectCanonical = catalog.canonicalOf(msg.objectClass()).orElse(msg.objectClass());

        switch (msg.messageType()) {
            case OBJECT -> processObject(key, value, msg, catalog, objectCanonical);
            case BEFORE_AFTER -> processBeforeAfter(key, value, msg, catalog, objectCanonical);
        }
    }

    // ==================== OBJECT (§11/§12) ====================

    private void processObject(String key, byte[] value, InputMessage msg,
                               MetamodelCatalog catalog, String objectCanonical) {
        JsonNode flat = msg.payload();
        List<CompiledSubscription> candidates = registry.all().stream()
                .filter(s -> s.matchesClass(objectCanonical, catalog))
                .filter(s -> !s.filter().preMatch(flat).isDefinitelyFalse())
                .toList();
        metrics.candidates(candidates.size());
        if (candidates.isEmpty()) {
            metrics.droppedNoCandidates();
            return;
        }

        List<String> requiredFields = unionRequiredFields(candidates);
        JsonNode enriched;
        try {
            enriched = backpressure.run(() ->
                    enrichClient.enrichObject(msg.objectClass(), msg.globalId(), requiredFields));
        } catch (EnrichException e) {
            dlqPublisher.toEnrichment(key, value, "enrich object failed: " + e.getMessage());
            return;
        }

        JsonNode merged = JsonMerge.deepMerge(flat, enriched);
        Predicate<String> present = f -> isPresent(merged, f, catalog);

        List<CompiledSubscription> matched = new ArrayList<>();
        for (CompiledSubscription sub : candidates) {
            if (!filterComputable(sub, present)) {
                dlqPublisher.toEnrichment(key, value,
                        "filter field missing after enrichment for " + sub.subscriptionId());
                return; // §28.6: cannot compute filter -> DLQ
            }
            if (sub.filter().matches(merged)) {
                matched.add(sub);
            }
        }
        if (matched.isEmpty()) {
            metrics.droppedNoMatches();
            return;
        }
        metrics.matched(matched.size());

        List<String> missing = missingFields(requiredFields, present);
        EnrichmentStatus status = missing.isEmpty() ? EnrichmentStatus.FULL : EnrichmentStatus.PARTIAL;
        if (status == EnrichmentStatus.PARTIAL) {
            metrics.partial();
        }

        ObjectNode out = mapper.createObjectNode();
        out.put("messageType", "OBJECT");
        out.put("sourceEventId", msg.sourceEventId());
        out.put("objectClass", msg.objectClass());
        out.put("objectId", msg.objectId());
        putLong(out, "globalId", msg.globalId());
        putLong(out, "revisionId", msg.revisionId());
        out.put("savedAt", msg.savedAt());
        ArrayNode ids = out.putArray("matchedSubscriptionIds");
        matched.forEach(s -> ids.add(s.subscriptionId()));
        out.set("payload", merged);
        out.set("metadata", metadata(status, missing));

        outputPublisher.publish(msg.objectId(), out, value);
    }

    // ==================== BEFORE_AFTER (§14/§16/§18) ====================

    private void processBeforeAfter(String key, byte[] value, InputMessage msg,
                                    MetamodelCatalog catalog, String objectCanonical) {
        JsonNode flatBefore = msg.before().payload();
        JsonNode flatAfter = msg.after().payload();

        // Candidate if class matches AND at least one side is not definitely false (§15).
        List<CompiledSubscription> candidates = registry.all().stream()
                .filter(s -> s.matchesClass(objectCanonical, catalog))
                .filter(s -> {
                    Tri before = s.filter().preMatch(flatBefore);
                    Tri after = s.filter().preMatch(flatAfter);
                    return !(before.isDefinitelyFalse() && after.isDefinitelyFalse());
                })
                .toList();
        metrics.candidates(candidates.size());
        if (candidates.isEmpty()) {
            metrics.droppedNoCandidates();
            return;
        }

        List<String> requiredFields = unionRequiredFields(candidates);
        long beforeRev = msg.before().revisionId();
        long afterRev = msg.after().revisionId();
        List<Long> revisions = beforeRev == afterRev ? List.of(beforeRev) : List.of(beforeRev, afterRev);

        Map<Long, JsonNode> enrichedByRevision;
        try {
            JsonNode response = backpressure.run(() ->
                    enrichClient.enrichRevisions(msg.objectClass(), revisions, requiredFields));
            enrichedByRevision = RevisionMatcher.match(response, revisions);
        } catch (EnrichException e) {
            dlqPublisher.toEnrichment(key, value, "enrich revisions failed: " + e.getMessage());
            return;
        }

        JsonNode enrichedBefore = JsonMerge.deepMerge(flatBefore, enrichedByRevision.get(beforeRev));
        JsonNode enrichedAfter = JsonMerge.deepMerge(flatAfter, enrichedByRevision.get(afterRev));
        Predicate<String> presentBoth = f ->
                isPresent(enrichedBefore, f, catalog) && isPresent(enrichedAfter, f, catalog);

        List<SubscriptionMatch> matches = new ArrayList<>();
        for (CompiledSubscription sub : candidates) {
            if (!filterComputable(sub, presentBoth)) {
                dlqPublisher.toEnrichment(key, value,
                        "filter field missing after enrichment for " + sub.subscriptionId());
                return; // §28.6
            }
            boolean beforeMatched = sub.filter().matches(enrichedBefore);
            boolean afterMatched = sub.filter().matches(enrichedAfter);
            if (beforeMatched || afterMatched) {
                matches.add(new SubscriptionMatch(sub.subscriptionId(), beforeMatched, afterMatched));
            }
        }
        if (matches.isEmpty()) {
            metrics.droppedNoMatches();
            return;
        }
        metrics.matched(matches.size());

        List<String> missing = missingFields(requiredFields, presentBoth);
        EnrichmentStatus status = missing.isEmpty() ? EnrichmentStatus.FULL : EnrichmentStatus.PARTIAL;
        if (status == EnrichmentStatus.PARTIAL) {
            metrics.partial();
        }

        ObjectNode out = mapper.createObjectNode();
        out.put("messageType", "BEFORE_AFTER");
        out.put("sourceEventId", msg.sourceEventId());
        out.put("objectClass", msg.objectClass());
        out.put("objectId", msg.objectId());
        out.put("savedAt", msg.savedAt());
        ArrayNode matchArray = out.putArray("subscriptionMatches");
        for (SubscriptionMatch m : matches) {
            ObjectNode mn = matchArray.addObject();
            mn.put("subscriptionId", m.subscriptionId());
            mn.put("beforeMatched", m.beforeMatched());
            mn.put("afterMatched", m.afterMatched());
        }
        out.set("before", version(msg.before().globalId(), beforeRev, enrichedBefore));
        out.set("after", version(msg.after().globalId(), afterRev, enrichedAfter));
        out.set("metadata", metadata(status, missing));

        outputPublisher.publish(msg.objectId(), out, value);
    }

    // ==================== helpers ====================

    private record SubscriptionMatch(String subscriptionId, boolean beforeMatched, boolean afterMatched) {
    }

    private List<String> unionRequiredFields(List<CompiledSubscription> subs) {
        TreeSet<String> union = new TreeSet<>();
        for (CompiledSubscription s : subs) {
            union.addAll(s.requiredFields());
        }
        return List.copyOf(union);
    }

    private boolean filterComputable(CompiledSubscription sub, Predicate<String> present) {
        for (String field : sub.filterFields()) {
            if (!present.test(field)) {
                return false;
            }
        }
        return true;
    }

    private List<String> missingFields(List<String> requiredFields, Predicate<String> present) {
        Set<String> missing = new LinkedHashSet<>();
        for (String f : requiredFields) {
            if (!present.test(f)) {
                missing.add(f);
            }
        }
        return List.copyOf(missing);
    }

    private boolean isPresent(JsonNode payload, String field, MetamodelCatalog catalog) {
        String[] segments = JsonPaths.jsonSegments(field, t -> catalog.canonicalOf(t).isPresent());
        return JsonPaths.readScalar(payload, segments) != null;
    }

    private ObjectNode metadata(EnrichmentStatus status, List<String> missing) {
        ObjectNode meta = mapper.createObjectNode();
        meta.put("enrichmentStatus", status.name());
        meta.put("enrichedAt", Instant.now().toString());
        if (!missing.isEmpty()) {
            ArrayNode mf = meta.putArray("missingFields");
            missing.forEach(mf::add);
        }
        return meta;
    }

    private ObjectNode version(Long globalId, long revisionId, JsonNode payload) {
        ObjectNode node = mapper.createObjectNode();
        putLong(node, "globalId", globalId);
        node.put("revisionId", revisionId);
        node.set("payload", payload);
        return node;
    }

    private static void putLong(ObjectNode node, String field, Long v) {
        if (v == null) {
            node.putNull(field);
        } else {
            node.put(field, v.longValue());
        }
    }
}
