package com.example.filterenrichment.kafka;

import com.example.filterenrichment.domain.EnrichmentStatus;
import com.example.filterenrichment.domain.InputMessage;
import com.example.filterenrichment.domain.InputMessageParser;
import com.example.filterenrichment.domain.InputParseException;
import com.example.filterenrichment.domain.MessageType;
import com.example.filterenrichment.enrich.EnrichClient;
import com.example.filterenrichment.enrich.EnrichException;
import com.example.filterenrichment.filter.JsonPaths;
import com.example.filterenrichment.metamodel.MetamodelCatalog;
import com.example.filterenrichment.metamodel.MetamodelHolder;
import com.example.filterenrichment.metrics.Metrics;
import com.example.filterenrichment.registry.CompiledSubscription;
import com.example.filterenrichment.registry.SubscriptionRegistry;
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
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;

/**
 * Core pipeline for the OBJECT_BATCH engine: parse & validate the input record, pre-match candidate
 * subscriptions on the flat payload, enrich once via the Object Enrich Service, apply full filters on
 * the enriched data, and publish a single matched output record — or drop / DLQ.
 *
 * <p>The engine emits ONE uniform output shape regardless of input shape. A before/after change is
 * processed as a plain object over its {@code after} state — the {@code before} side is ignored and
 * never emitted — so the published record is byte-for-byte the same envelope as a single OBJECT
 * message ({@code matchedSubscriptionIds} + {@code object} + {@code metadata}).
 *
 * <p>Never treats a filter as false merely because a field is missing: if a filter cannot be computed
 * the message goes to the Enrichment DLQ.
 */
@Component
public class MessageProcessor {

    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);

    private final ObjectMapper mapper;
    private final InputMessageParser parser;
    private final SubscriptionRegistry registry;
    private final MetamodelHolder metamodel;
    private final EnrichClient enrichClient;
    private final BackpressureManager backpressure;
    private final OutputPublisher outputPublisher;
    private final DlqPublisher dlqPublisher;
    private final Metrics metrics;

    public MessageProcessor(ObjectMapper mapper,
                            InputMessageParser parser,
                            SubscriptionRegistry registry,
                            MetamodelHolder metamodel,
                            EnrichClient enrichClient,
                            BackpressureManager backpressure,
                            OutputPublisher outputPublisher,
                            DlqPublisher dlqPublisher,
                            Metrics metrics) {
        this.mapper = mapper;
        this.parser = parser;
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
            msg = parser.parse(value);
        } catch (InputParseException e) {
            dlqPublisher.toInput(key, value, e.getMessage());
            return;
        }
        metrics.inputType(msg.messageType());
        MDC.put("messageType", String.valueOf(msg.messageType()));
        MDC.put("objectClass", String.valueOf(msg.objectClass()));

        MetamodelCatalog catalog = metamodel.get(); // throws if not loaded -> record redelivered
        String objectCanonical = catalog.canonicalOf(msg.objectClass()).orElse(msg.objectClass());

        // OBJECT_BATCH is state-oriented: a before/after change is handled as a single object over its
        // `after` state (the `before` side is dropped, never emitted). The parser already resolves
        // objectClass/globalId/objectId to the `after` version, so the flat payload for pre-matching is
        // the only thing to pick per input shape — everything downstream is the exact OBJECT path.
        JsonNode flat = msg.messageType() == MessageType.BEFORE_AFTER
                ? msg.after().payload()
                : msg.payload();

        processObject(key, value, msg, catalog, objectCanonical, flat);
    }

    private void processObject(String key, byte[] value, InputMessage msg,
                               MetamodelCatalog catalog, String objectCanonical, JsonNode flat) {
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

        // The enriched object is self-describing (carries objectClass/globalId/id/revision/savedAt and
        // the flat scalars) — evaluate the full filter and emit it directly, no merge with the source.
        Predicate<String> present = f -> isPresent(enriched, f, catalog);

        List<CompiledSubscription> matched = new ArrayList<>();
        for (CompiledSubscription sub : candidates) {
            if (!filterComputable(sub, present)) {
                dlqPublisher.toEnrichment(key, value,
                        "filter field missing after enrichment for " + sub.subscriptionId());
                return; // cannot compute filter -> DLQ
            }
            if (sub.filter().matches(enriched)) {
                matched.add(sub);
            }
        }
        if (matched.isEmpty()) {
            metrics.droppedNoMatches();
            return;
        }
        metrics.matched(matched.size());

        List<String> missing = missingFields(requiredFields, present);
        EnrichmentStatus status = statusOf(missing);

        ObjectNode out = mapper.createObjectNode();
        ArrayNode ids = out.putArray("matchedSubscriptionIds");
        matched.forEach(s -> ids.add(s.subscriptionId()));
        out.set("object", enriched);
        out.set("metadata", metadata(status, missing));

        outputPublisher.publish(msg.objectId(), out, value);
    }

    // ==================== helpers ====================

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

    private EnrichmentStatus statusOf(List<String> missing) {
        if (missing.isEmpty()) {
            return EnrichmentStatus.FULL;
        }
        metrics.partial();
        return EnrichmentStatus.PARTIAL;
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
}
