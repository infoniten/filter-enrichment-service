package com.example.filterenrichment.registry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * In-memory registry of served, compiled subscriptions held by each pod. Populated at startup
 * and kept current via Redis Pub/Sub. Filters are compiled once, on load/change.
 */
@Component
public class SubscriptionRegistry {

    private static final Logger log = LoggerFactory.getLogger(SubscriptionRegistry.class);

    private final ConcurrentMap<String, CompiledSubscription> byId = new ConcurrentHashMap<>();

    public void put(CompiledSubscription subscription) {
        byId.put(subscription.subscriptionId(), subscription);
        log.debug("Registry upsert: {}", subscription.subscriptionId());
    }

    public void remove(String subscriptionId) {
        if (byId.remove(subscriptionId) != null) {
            log.debug("Registry remove: {}", subscriptionId);
        }
    }

    public void clear() {
        byId.clear();
    }

    public Collection<CompiledSubscription> all() {
        return byId.values();
    }

    public int size() {
        return byId.size();
    }
}
