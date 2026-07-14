package com.example.filterenrichment.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

/**
 * Runtime configuration of a subscription as stored by the Subscription Service in Redis under
 * {@code sub:{subscriptionId}} (§5.1). Only {@code status == ACTIVE} subscriptions are used; the
 * service never touches Postgres.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record RuntimeSubscription(
        String subscriptionId,
        String subscriberName,
        String topicPostfix,
        String topicName,
        String objectClass,
        List<String> fields,
        String filter,
        String engine,
        String status,
        String createdAt
) {

    public boolean isActive() {
        return "ACTIVE".equals(status);
    }

    public boolean hasEngine(String servedEngine) {
        return servedEngine != null && servedEngine.equals(engine);
    }
}
