package com.example.filterenrichment.kafka;

import com.example.filterenrichment.TestFixtures;
import com.example.filterenrichment.domain.InputMessageParser;
import com.example.filterenrichment.domain.RuntimeSubscription;
import com.example.filterenrichment.enrich.EnrichClient;
import com.example.filterenrichment.enrich.EnrichException;
import com.example.filterenrichment.filter.RsqlFilterCompiler;
import com.example.filterenrichment.metamodel.MetamodelCatalog;
import com.example.filterenrichment.metamodel.MetamodelHolder;
import com.example.filterenrichment.metrics.Metrics;
import com.example.filterenrichment.registry.SubscriptionCompiler;
import com.example.filterenrichment.registry.SubscriptionRegistry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.assertj.core.api.Assertions.assertThat;

class MessageProcessorTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final MetamodelCatalog catalog = TestFixtures.catalog();

    private SubscriptionRegistry registry;
    private EnrichClient enrichClient;
    private OutputPublisher outputPublisher;
    private DlqPublisher dlqPublisher;
    private BackpressureManager backpressure;
    private MessageProcessor processor;

    @BeforeEach
    void setUp() {
        registry = new SubscriptionRegistry();
        enrichClient = mock(EnrichClient.class);
        outputPublisher = mock(OutputPublisher.class);
        dlqPublisher = mock(DlqPublisher.class);
        backpressure = mock(BackpressureManager.class);
        when(backpressure.run(any())).thenAnswer(inv -> ((Supplier<?>) inv.getArgument(0)).get());

        MetamodelHolder metamodel = mock(MetamodelHolder.class);
        when(metamodel.get()).thenReturn(catalog);

        processor = new MessageProcessor(mapper, new InputMessageParser(mapper), registry, metamodel,
                enrichClient, backpressure, outputPublisher, dlqPublisher, new Metrics(new SimpleMeterRegistry()));
    }

    private void register(String id, String objectClass, List<String> fields, String filter) {
        SubscriptionCompiler compiler = new SubscriptionCompiler(new RsqlFilterCompiler());
        RuntimeSubscription sub = new RuntimeSubscription(id, "risk", "prod", "subscription.risk.prod",
                List.of(new RuntimeSubscription.Target(objectClass, true)),
                fields, filter, "OBJECT_BATCH", "ACTIVE", "2026-07-13T10:00:00Z");
        registry.put(compiler.compile(sub, catalog));
    }

    // ==================== OBJECT ====================

    @Test
    void objectMatched_publishesWithMatchedSubscriptionIds() {
        register("sub-1", "FxSpotForwardTrade", List.of("Trade.counterparty.name"),
                "portfolioId==1;Trade.counterparty.code==ACME");
        // Enrich returns the self-describing object: flat scalars + resolved relations.
        when(enrichClient.enrichObject(eq("FxSpotForwardTrade"), eq(123L), anyList()))
                .thenReturn(TestFixtures.json(
                        "{\"portfolioId\":1,\"status\":\"ACTIVE\",\"counterparty\":{\"code\":\"ACME\",\"name\":\"ACME BANK\"}}"));

        processor.process("t1", objectMessage());

        ArgumentCaptor<JsonNode> out = ArgumentCaptor.forClass(JsonNode.class);
        verify(outputPublisher).publish(eq("123"), out.capture(), any()); // keyed by globalId
        JsonNode published = out.getValue();
        assertThat(published.get("matchedSubscriptionIds")).hasSize(1);
        assertThat(published.get("matchedSubscriptionIds").get(0).asText()).isEqualTo("sub-1");
        assertThat(published.get("object").get("counterparty").get("code").asText()).isEqualTo("ACME");
        assertThat(published.get("metadata").get("enrichmentStatus").asText()).isEqualTo("FULL");
    }

    @Test
    void objectNoCandidates_dropsWithoutEnrichmentOrOutput() {
        register("sub-1", "FxSpotForwardTrade", List.of("Trade.contractId"), "portfolioId==999");
        // flat portfolioId=1 -> definitely false -> not a candidate
        processor.process("t1", objectMessage());

        verify(enrichClient, never()).enrichObject(any(), anyLong(), anyList());
        verify(outputPublisher, never()).publish(any(), any(), any());
    }

    @Test
    void objectNoMatchAfterEnrichment_dropsNoOutput() {
        register("sub-1", "FxSpotForwardTrade", List.of("Trade.counterparty.name"),
                "Trade.counterparty.code==NOPE");
        when(enrichClient.enrichObject(any(), anyLong(), anyList()))
                .thenReturn(TestFixtures.json("{\"counterparty\":{\"code\":\"ACME\",\"name\":\"ACME BANK\"}}"));

        processor.process("t1", objectMessage());

        verify(outputPublisher, never()).publish(any(), any(), any());
    }

    @Test
    void objectEnrichFailure_routesToEnrichmentDlq() {
        register("sub-1", "FxSpotForwardTrade", List.of("Trade.counterparty.name"),
                "Trade.counterparty.code==ACME");
        when(enrichClient.enrichObject(any(), anyLong(), anyList()))
                .thenThrow(new EnrichException(EnrichException.Kind.NOT_FOUND, "404"));

        byte[] value = objectMessage();
        processor.process("t1", value);

        verify(dlqPublisher).toEnrichment(eq("t1"), eq(value), any());
        verify(outputPublisher, never()).publish(any(), any(), any());
    }

    @Test
    void malformedMessage_routesToInputDlq() {
        byte[] value = "not json".getBytes(StandardCharsets.UTF_8);
        processor.process("t1", value);
        verify(dlqPublisher).toInput(eq("t1"), eq(value), any());
    }

    // ============ BEFORE_AFTER collapsed to the `after` object (single uniform format) ============

    @Test
    void beforeAfter_emittedAsSingleObject_usingAfterState() {
        // Filter matches the `after` state (portfolioId=2); the `before` side (portfolioId=1) is ignored.
        register("sub-1", "FxSpotForwardTrade", List.of("Trade.contractId"), "portfolioId==2");
        // Enriched by globalId exactly like a single OBJECT message — no revision-pair call.
        when(enrichClient.enrichObject(eq("FxSpotForwardTrade"), eq(123L), anyList()))
                .thenReturn(TestFixtures.json("{\"portfolioId\":2,\"contractId\":1}"));

        processor.process("t1", beforeAfterMessage());

        ArgumentCaptor<JsonNode> out = ArgumentCaptor.forClass(JsonNode.class);
        verify(outputPublisher).publish(eq("123"), out.capture(), any()); // keyed by globalId of `after`
        JsonNode published = out.getValue();
        // Same envelope as a single OBJECT: matchedSubscriptionIds + object + metadata.
        assertThat(published.get("matchedSubscriptionIds")).hasSize(1);
        assertThat(published.get("matchedSubscriptionIds").get(0).asText()).isEqualTo("sub-1");
        assertThat(published.get("object").get("portfolioId").asInt()).isEqualTo(2);
        assertThat(published.get("metadata").get("enrichmentStatus").asText()).isEqualTo("FULL");
        // No before/after or per-side match flags leak into the output.
        assertThat(published.hasNonNull("before")).isFalse();
        assertThat(published.hasNonNull("after")).isFalse();
        assertThat(published.hasNonNull("subscriptionMatches")).isFalse();
    }

    @Test
    void beforeAfter_afterNotCandidate_dropsWithoutEnrichOrOutput() {
        // Filter matches only the `before` state; since only `after` is considered, it is dropped.
        register("sub-1", "FxSpotForwardTrade", List.of("Trade.contractId"), "portfolioId==1");

        processor.process("t1", beforeAfterMessage());

        verify(enrichClient, never()).enrichObject(any(), anyLong(), anyList());
        verify(outputPublisher, never()).publish(any(), any(), any());
    }

    // Real source format: no envelope/messageType/payload; flat object; objectType (soon objectClass).
    private byte[] objectMessage() {
        return ("""
                {"objectType":"FxSpotForwardTrade","globalId":123,"id":987,"revision":5,
                 "revisionEventId":"110640124","savedAt":"2026-07-13T10:15:30Z",
                 "portfolioId":1,"status":"ACTIVE"}
                """).getBytes(StandardCharsets.UTF_8);
    }

    private byte[] beforeAfterMessage() {
        return ("""
                {"before":{"objectType":"FxSpotForwardTrade","globalId":123,"id":10,"revision":1666,
                           "revisionEventId":"110640123","savedAt":"2026-07-13T10:19:00Z",
                           "portfolioId":1,"status":"ACTIVE"},
                 "after":{"objectType":"FxSpotForwardTrade","globalId":123,"id":11,"revision":1667,
                          "revisionEventId":"110640124","savedAt":"2026-07-13T10:20:00Z",
                          "portfolioId":2,"status":"ACTIVE"}}
                """).getBytes(StandardCharsets.UTF_8);
    }
}
