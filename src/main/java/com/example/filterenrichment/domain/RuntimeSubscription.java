package com.example.filterenrichment.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

/**
 * Runtime configuration of a subscription as stored by the Subscription Service in Redis under
 * {@code sub:{subscriptionId}}. Only {@code status == ACTIVE} subscriptions are used; the
 * service never touches Postgres.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record RuntimeSubscription(
        String subscriptionId,
        String subscriberName,
        String topicPostfix,
        String topicName,
        List<Target> targets,
        List<String> fields,
        String filter,
        String engine,
        String status,
        String createdAt
) {

    /** One class target: objectClass + whether subclasses are included (polymorphic vs exact). */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Target(String objectClass, boolean includeSubclasses) {
    }

    public boolean isActive() {
        return "ACTIVE".equals(status);
    }

    public boolean hasEngine(String servedEngine) {
        return servedEngine != null && servedEngine.equals(engine);
    }
}
