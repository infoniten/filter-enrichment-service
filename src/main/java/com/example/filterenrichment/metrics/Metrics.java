package com.example.filterenrichment.metrics;

import com.example.filterenrichment.domain.MessageType;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

/**
 * All service metrics (§35). Never uses {@code subscriptionId} as a label (unbounded cardinality).
 * Gauges for active subscriptions / in-flight / paused partitions are bound at their source
 * (registry, backpressure manager, listener) rather than here.
 */
@Component
public class Metrics {

    private final MeterRegistry registry;

    private final Counter input;
    private final Counter inputObject;
    private final Counter inputBeforeAfter;
    private final Counter droppedNoCandidates;
    private final Counter droppedNoMatches;
    private final DistributionSummary candidateSubscriptions;
    private final DistributionSummary matchedSubscriptions;
    private final Counter getRequests;
    private final Counter revisionRequests;
    private final Counter httpErrors;
    private final Timer httpLatency;
    private final Counter partial;
    private final Counter output;
    private final Counter retry;
    private final Counter configReload;

    public Metrics(MeterRegistry registry) {
        this.registry = registry;
        this.input = Counter.builder("filter_enrichment_input_total").register(registry);
        this.inputObject = Counter.builder("filter_enrichment_input_by_type_total").tag("type", "OBJECT").register(registry);
        this.inputBeforeAfter = Counter.builder("filter_enrichment_input_by_type_total").tag("type", "BEFORE_AFTER").register(registry);
        this.droppedNoCandidates = Counter.builder("filter_enrichment_dropped_no_candidates_total").register(registry);
        this.droppedNoMatches = Counter.builder("filter_enrichment_dropped_no_matches_total").register(registry);
        this.candidateSubscriptions = DistributionSummary.builder("filter_enrichment_candidate_subscriptions_total").register(registry);
        this.matchedSubscriptions = DistributionSummary.builder("filter_enrichment_matched_subscriptions_total").register(registry);
        this.getRequests = Counter.builder("filter_enrichment_get_requests_total").register(registry);
        this.revisionRequests = Counter.builder("filter_enrichment_revision_requests_total").register(registry);
        this.httpErrors = Counter.builder("filter_enrichment_http_errors_total").register(registry);
        this.httpLatency = Timer.builder("filter_enrichment_http_latency_seconds").register(registry);
        this.partial = Counter.builder("filter_enrichment_partial_total").register(registry);
        this.output = Counter.builder("filter_enrichment_output_total").register(registry);
        this.retry = Counter.builder("filter_enrichment_retry_total").register(registry);
        this.configReload = Counter.builder("filter_enrichment_config_reload_total").register(registry);
    }

    /** One record received (§35 {@code filter_enrichment_input_total}), before type is known. */
    public void inputReceived() {
        input.increment();
    }

    /** Record counted by its resolved type (§35 {@code filter_enrichment_input_by_type_total}). */
    public void inputType(MessageType type) {
        if (type == MessageType.BEFORE_AFTER) {
            inputBeforeAfter.increment();
        } else {
            inputObject.increment();
        }
    }

    public void droppedNoCandidates() {
        droppedNoCandidates.increment();
    }

    public void droppedNoMatches() {
        droppedNoMatches.increment();
    }

    public void candidates(int n) {
        candidateSubscriptions.record(n);
    }

    public void matched(int n) {
        matchedSubscriptions.record(n);
    }

    public void getRequest() {
        getRequests.increment();
    }

    public void revisionRequest() {
        revisionRequests.increment();
    }

    public void httpError() {
        httpErrors.increment();
    }

    public Timer httpLatency() {
        return httpLatency;
    }

    public void partial() {
        partial.increment();
    }

    public void output() {
        output.increment();
    }

    public void retry() {
        retry.increment();
    }

    public void configReload() {
        configReload.increment();
    }

    /** Increments {@code filter_enrichment_http_requests_total} tagged by outcome. */
    public void httpRequest(String outcome) {
        Counter.builder("filter_enrichment_http_requests_total").tag("outcome", outcome).register(registry).increment();
    }

    /** Increments {@code filter_enrichment_dlq_total} tagged by which DLQ received the message. */
    public void dlq(String queue) {
        Counter.builder("filter_enrichment_dlq_total").tag("dlq", queue).register(registry).increment();
    }
}
