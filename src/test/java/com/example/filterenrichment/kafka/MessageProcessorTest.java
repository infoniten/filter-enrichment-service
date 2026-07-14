package com.example.filterenrichment.kafka;

import com.example.filterenrichment.TestFixtures;
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

        processor = new MessageProcessor(mapper, registry, metamodel, enrichClient,
                backpressure, outputPublisher, dlqPublisher, new Metrics(new SimpleMeterRegistry()));
    }

    private void register(String id, String objectClass, List<String> fields, String filter) {
        SubscriptionCompiler compiler = new SubscriptionCompiler(new RsqlFilterCompiler());
        RuntimeSubscription sub = new RuntimeSubscription(id, "risk", "prod",
                "subscription.risk.prod", objectClass, fields, filter, "OBJECT_BATCH", "ACTIVE", "2026-07-13T10:00:00Z");
        registry.put(compiler.compile(sub, catalog));
    }

    // ==================== OBJECT ====================

    @Test
    void objectMatched_publishesWithMatchedSubscriptionIds() {
        register("sub-1", "FxSpotForwardTrade", List.of("Trade.counterparty.name"),
                "portfolioId==1;Trade.counterparty.code==ACME");
        when(enrichClient.enrichObject(eq("FxSpotForwardTrade"), eq(123L), anyList()))
                .thenReturn(TestFixtures.json("{\"counterparty\":{\"code\":\"ACME\",\"name\":\"ACME BANK\"}}"));

        processor.process("t1", objectMessage());

        ArgumentCaptor<JsonNode> out = ArgumentCaptor.forClass(JsonNode.class);
        verify(outputPublisher).publish(eq("t1"), out.capture(), any());
        JsonNode published = out.getValue();
        assertThat(published.get("messageType").asText()).isEqualTo("OBJECT");
        assertThat(published.get("matchedSubscriptionIds")).hasSize(1);
        assertThat(published.get("matchedSubscriptionIds").get(0).asText()).isEqualTo("sub-1");
        assertThat(published.get("payload").get("counterparty").get("code").asText()).isEqualTo("ACME");
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

    // ==================== BEFORE_AFTER ====================

    @Test
    void beforeAfter_beforeMatchesOnly_recordsBothFlags() {
        register("sub-1", "FxSpotForwardTrade", List.of("Trade.contractId"), "portfolioId==1");
        when(enrichClient.enrichRevisions(eq("FxSpotForwardTrade"), eq(List.of(10L, 11L)), anyList()))
                .thenReturn(TestFixtures.json("[{\"revisionId\":10,\"contractId\":1},{\"revisionId\":11,\"contractId\":1}]"));

        processor.process("t1", beforeAfterMessage());

        ArgumentCaptor<JsonNode> out = ArgumentCaptor.forClass(JsonNode.class);
        verify(outputPublisher).publish(eq("t1"), out.capture(), any());
        JsonNode m = out.getValue().get("subscriptionMatches").get(0);
        assertThat(m.get("subscriptionId").asText()).isEqualTo("sub-1");
        assertThat(m.get("beforeMatched").asBoolean()).isTrue();
        assertThat(m.get("afterMatched").asBoolean()).isFalse();
    }

    @Test
    void beforeAfter_missingRevision_routesToEnrichmentDlq() {
        register("sub-1", "FxSpotForwardTrade", List.of("Trade.contractId"), "portfolioId==1");
        when(enrichClient.enrichRevisions(any(), anyList(), anyList()))
                .thenReturn(TestFixtures.json("[{\"revisionId\":10,\"contractId\":1}]")); // rev 11 missing

        byte[] value = beforeAfterMessage();
        processor.process("t1", value);

        verify(dlqPublisher).toEnrichment(eq("t1"), eq(value), any());
        verify(outputPublisher, never()).publish(any(), any(), any());
    }

    private byte[] objectMessage() {
        return ("""
                {"messageType":"OBJECT","sourceEventId":"e1","objectClass":"FxSpotForwardTrade",
                 "objectId":"t1","globalId":123,"revisionId":987,"savedAt":"2026-07-13T10:15:30Z",
                 "payload":{"portfolioId":1,"status":"ACTIVE"}}
                """).getBytes(StandardCharsets.UTF_8);
    }

    private byte[] beforeAfterMessage() {
        return ("""
                {"messageType":"BEFORE_AFTER","sourceEventId":"e2","objectClass":"FxSpotForwardTrade",
                 "objectId":"t1","savedAt":"2026-07-13T10:20:00Z",
                 "before":{"globalId":123,"revisionId":10,"payload":{"portfolioId":1,"status":"ACTIVE"}},
                 "after":{"globalId":123,"revisionId":11,"payload":{"portfolioId":2,"status":"ACTIVE"}}}
                """).getBytes(StandardCharsets.UTF_8);
    }
}
