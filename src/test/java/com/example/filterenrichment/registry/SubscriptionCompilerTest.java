package com.example.filterenrichment.registry;

import com.example.filterenrichment.TestFixtures;
import com.example.filterenrichment.domain.RuntimeSubscription;
import com.example.filterenrichment.domain.RuntimeSubscription.Target;
import com.example.filterenrichment.filter.FilterCompileException;
import com.example.filterenrichment.filter.RsqlFilterCompiler;
import com.example.filterenrichment.metamodel.MetamodelCatalog;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SubscriptionCompilerTest {

    private final MetamodelCatalog catalog = TestFixtures.catalog();
    private final SubscriptionCompiler compiler = new SubscriptionCompiler(new RsqlFilterCompiler());

    private RuntimeSubscription sub(List<Target> targets) {
        return new RuntimeSubscription("sub-1", "risk", "prod", "subscription.risk.prod",
                targets, List.of("Trade.portfolioId"), "Trade.portfolioId==6052",
                "OBJECT_BATCH", "ACTIVE", "2026-07-14T10:00:00Z");
    }

    @Test
    void compilesTargetsToCanonical() {
        CompiledSubscription compiled = compiler.compile(
                sub(List.of(new Target("FxSpotForwardTrade", true), new Target("Currency", false))), catalog);
        assertThat(compiled.targets()).containsExactly(
                new CompiledSubscription.CompiledTarget("FX_SPOT_FORWARD_TRADE", true),
                new CompiledSubscription.CompiledTarget("CURRENCY", false));
    }

    @Test
    void polymorphicTargetMatchesSubclass_exactDoesNot() {
        // Trade with SUBTREE matches FxSpotForwardTrade (a subclass); EXACT Trade does not.
        assertThat(compiler.compile(sub(List.of(new Target("Trade", true))), catalog)
                .matchesClass("FX_SPOT_FORWARD_TRADE", catalog)).isTrue();
        assertThat(compiler.compile(sub(List.of(new Target("Trade", false))), catalog)
                .matchesClass("FX_SPOT_FORWARD_TRADE", catalog)).isFalse();
        // EXACT Trade matches exactly Trade.
        assertThat(compiler.compile(sub(List.of(new Target("Trade", false))), catalog)
                .matchesClass("TRADE", catalog)).isTrue();
    }

    @Test
    void multiClassMatchesAnyTarget() {
        CompiledSubscription compiled = compiler.compile(
                sub(List.of(new Target("Currency", false), new Target("FxSpotForwardTrade", true))), catalog);
        assertThat(compiled.matchesClass("CURRENCY", catalog)).isTrue();
        assertThat(compiled.matchesClass("FX_SPOT_FORWARD_TRADE", catalog)).isTrue();
        assertThat(compiled.matchesClass("TRADE", catalog)).isFalse(); // neither target selects Trade
    }

    @Test
    void rejectsSubscriptionWithoutTargets() {
        assertThatThrownBy(() -> compiler.compile(sub(null), catalog))
                .isInstanceOf(FilterCompileException.class)
                .satisfies(e -> assertThat(((FilterCompileException) e).getReason()).isEqualTo("MISSING_TARGETS"));
        assertThatThrownBy(() -> compiler.compile(sub(List.of()), catalog))
                .isInstanceOf(FilterCompileException.class);
    }

    @Test
    void rejectsTargetWithoutObjectClass() {
        assertThatThrownBy(() -> compiler.compile(sub(List.of(new Target("  ", true))), catalog))
                .isInstanceOf(FilterCompileException.class)
                .satisfies(e -> assertThat(((FilterCompileException) e).getReason()).isEqualTo("MISSING_OBJECT_CLASS"));
    }
}
