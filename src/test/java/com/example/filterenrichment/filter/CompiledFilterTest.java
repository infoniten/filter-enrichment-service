package com.example.filterenrichment.filter;

import com.example.filterenrichment.TestFixtures;
import com.example.filterenrichment.metamodel.MetamodelCatalog;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CompiledFilterTest {

    private final MetamodelCatalog catalog = TestFixtures.catalog();
    private final RsqlFilterCompiler compiler = new RsqlFilterCompiler();

    @Test
    void emptyFilterAlwaysMatches() {
        CompiledFilter f = compiler.compile(null, catalog);
        assertThat(f.matches(TestFixtures.json("{}"))).isTrue();
        assertThat(f.preMatch(TestFixtures.json("{}"))).isEqualTo(Tri.TRUE);
    }

    @Test
    void flatFieldPreMatchIsDecidableFromFlatPayload() {
        CompiledFilter f = compiler.compile("portfolioId==1", catalog);
        assertThat(f.preMatch(TestFixtures.json("{\"portfolioId\":1}"))).isEqualTo(Tri.TRUE);
        assertThat(f.preMatch(TestFixtures.json("{\"portfolioId\":2}"))).isEqualTo(Tri.FALSE);
        // Absent in flat payload -> undecidable (§15).
        assertThat(f.preMatch(TestFixtures.json("{}"))).isEqualTo(Tri.UNKNOWN);
    }

    @Test
    void enrichedOnlyFieldIsUnknownInPreMatchButEvaluatedOnEnriched() {
        CompiledFilter f = compiler.compile("Trade.counterparty.code==ACME", catalog);
        // Not present in flat payload -> UNKNOWN (must stay a candidate).
        assertThat(f.preMatch(TestFixtures.json("{\"portfolioId\":1}"))).isEqualTo(Tri.UNKNOWN);
        // Present after enrichment -> evaluated fully.
        assertThat(f.matches(TestFixtures.json("{\"counterparty\":{\"code\":\"ACME\"}}"))).isTrue();
        assertThat(f.matches(TestFixtures.json("{\"counterparty\":{\"code\":\"OTHER\"}}"))).isFalse();
    }

    @Test
    void kleeneAndOr() {
        // AND: flat-false dominates -> FALSE even though the other leaf is UNKNOWN.
        CompiledFilter and = compiler.compile("portfolioId==1;Trade.counterparty.code==ACME", catalog);
        assertThat(and.preMatch(TestFixtures.json("{\"portfolioId\":2}"))).isEqualTo(Tri.FALSE);
        assertThat(and.preMatch(TestFixtures.json("{\"portfolioId\":1}"))).isEqualTo(Tri.UNKNOWN);

        // OR: flat-true dominates -> TRUE even though the other leaf is UNKNOWN.
        CompiledFilter or = compiler.compile("portfolioId==1,Trade.counterparty.code==ACME", catalog);
        assertThat(or.preMatch(TestFixtures.json("{\"portfolioId\":1}"))).isEqualTo(Tri.TRUE);
        assertThat(or.preMatch(TestFixtures.json("{\"portfolioId\":2}"))).isEqualTo(Tri.UNKNOWN);
    }

    @Test
    void filterTraversingCollectionFailsToCompile() {
        assertThatThrownBy(() -> compiler.compile("Trade.cashflows.amount==5", catalog))
                .isInstanceOf(FilterCompileException.class)
                .satisfies(e -> assertThat(((FilterCompileException) e).getReason())
                        .isEqualTo("FILTER_TRAVERSES_COLLECTION"));
    }

    @Test
    void filterOnUnknownQualifiedFieldFailsToCompile() {
        assertThatThrownBy(() -> compiler.compile("Trade.nope==1", catalog))
                .isInstanceOf(FilterCompileException.class)
                .satisfies(e -> assertThat(((FilterCompileException) e).getReason())
                        .isEqualTo("FILTER_SCHEMA_MISMATCH"));
    }
}
