package com.example.filterenrichment.enrich;

import com.example.filterenrichment.TestFixtures;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RevisionMatcherTest {

    @Test
    void matchesArrayResponseByRevisionIdRegardlessOfOrder() {
        JsonNode resp = TestFixtures.json("""
                [
                  {"revisionId": 987654, "contractId": 2},
                  {"revisionId": 987653, "contractId": 1}
                ]
                """);
        Map<Long, JsonNode> byRev = RevisionMatcher.match(resp, List.of(987653L, 987654L));
        assertThat(byRev.get(987653L).get("contractId").asInt()).isEqualTo(1);
        assertThat(byRev.get(987654L).get("contractId").asInt()).isEqualTo(2);
    }

    @Test
    void matchesObjectKeyedResponse() {
        JsonNode resp = TestFixtures.json("{\"987653\": {\"contractId\": 1}, \"987654\": {\"contractId\": 2}}");
        Map<Long, JsonNode> byRev = RevisionMatcher.match(resp, List.of(987653L, 987654L));
        assertThat(byRev).containsKeys(987653L, 987654L);
    }

    @Test
    void missingRevisionIsRejected() {
        JsonNode resp = TestFixtures.json("[{\"revisionId\": 987653}]");
        assertThatThrownBy(() -> RevisionMatcher.match(resp, List.of(987653L, 987654L)))
                .isInstanceOf(EnrichException.class)
                .satisfies(e -> assertThat(((EnrichException) e).kind()).isEqualTo(EnrichException.Kind.NON_RETRYABLE));
    }

    @Test
    void duplicateRevisionIsRejected() {
        JsonNode resp = TestFixtures.json("[{\"revisionId\": 987653}, {\"revisionId\": 987653}]");
        assertThatThrownBy(() -> RevisionMatcher.match(resp, List.of(987653L)))
                .isInstanceOf(EnrichException.class);
    }
}
