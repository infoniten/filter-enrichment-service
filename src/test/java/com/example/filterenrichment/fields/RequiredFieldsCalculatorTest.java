package com.example.filterenrichment.fields;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RequiredFieldsCalculatorTest {

    @Test
    void unionsFieldsAndFilterFieldsDeduplicatedAndSorted() {
        List<String> fields = List.of("Trade.contractId", "Trade.counterparty.name", "Trade.contractId");
        String filter = "portfolioId==1;Trade.counterparty.code==ACME";

        List<String> result = RequiredFieldsCalculator.compute(fields, filter);

        // Deterministic (lexicographic), de-duplicated; the flat `portfolioId` is not an outputField.
        assertThat(result).containsExactly(
                "Trade.contractId",
                "Trade.counterparty.code",
                "Trade.counterparty.name");
    }

    @Test
    void handlesNullFieldsAndFilter() {
        assertThat(RequiredFieldsCalculator.compute(null, null)).isEmpty();
    }
}
