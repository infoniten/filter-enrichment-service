package com.example.filterenrichment.registry;

import com.example.filterenrichment.client.SubscriptionFailClient;
import com.example.filterenrichment.config.FilterEnrichmentProperties;
import com.example.filterenrichment.domain.RuntimeSubscription;
import com.example.filterenrichment.filter.FilterCompileException;
import com.example.filterenrichment.metamodel.MetamodelHolder;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Loads and refreshes the compiled runtime subscription registry from Redis. The service
 * only ever reads Redis — never Postgres. A full {@link #loadAll()} happens at startup and on a
 * signal with no {@code subscriptionId}; {@link #refreshOne(String)} handles a single change.
 */
@Service
public class RuntimeConfigService {

    private static final Logger log = LoggerFactory.getLogger(RuntimeConfigService.class);

    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    private final SubscriptionRegistry registry;
    private final SubscriptionCompiler compiler;
    private final MetamodelHolder metamodel;
    private final SubscriptionFailClient failClient;
    private final FilterEnrichmentProperties props;
    private final AtomicBoolean loaded = new AtomicBoolean(false);

    public RuntimeConfigService(StringRedisTemplate redis,
                                ObjectMapper objectMapper,
                                SubscriptionRegistry registry,
                                SubscriptionCompiler compiler,
                                MetamodelHolder metamodel,
                                SubscriptionFailClient failClient,
                                FilterEnrichmentProperties props) {
        this.redis = redis;
        this.objectMapper = objectMapper;
        this.registry = registry;
        this.compiler = compiler;
        this.metamodel = metamodel;
        this.failClient = failClient;
        this.props = props;
    }

    /** Full load of the runtime set at startup / resync (requires metadata to be loaded first). */
    public void loadAll() {
        Set<String> ids = redis.opsForSet().members(props.getRedis().getRuntimeSetKey());
        registry.clear();
        int served = 0;
        if (ids != null) {
            for (String id : ids) {
                if (refreshOne(id)) {
                    served++;
                }
            }
        }
        loaded.set(true);
        log.info("Loaded runtime subscriptions: {} in set, {} served ({})",
                ids == null ? 0 : ids.size(), served, props.getServedEngine());
    }

    /**
     * Reloads and recompiles a single subscription's config. Returns true if it is now
     * served (present in the registry).
     */
    public boolean refreshOne(String subscriptionId) {
        String json = redis.opsForValue().get(configKey(subscriptionId));
        if (json == null) {
            registry.remove(subscriptionId); // removed from Redis -> drop
            return false;
        }
        RuntimeSubscription sub = parse(json);
        if (sub == null) {
            registry.remove(subscriptionId);
            return false;
        }
        if (!sub.isActive() || !sub.hasEngine(props.getServedEngine())) {
            registry.remove(subscriptionId); // only ACTIVE + served engine
            return false;
        }
        try {
            registry.put(compiler.compile(sub, metamodel.get()));
            return true;
        } catch (FilterCompileException e) {
            // filter cannot be compiled -> fail the subscription and drop it from this pod
            log.warn("Filter compilation failed for {}: {}", subscriptionId, e.getMessage());
            registry.remove(subscriptionId);
            failClient.fail(subscriptionId, e.getReason(), e.getMessage());
            return false;
        } catch (IllegalStateException e) {
            // Metadata not loaded yet (transient) — do not fail the subscription; retry on next load
            log.warn("Cannot compile {} yet: {}", subscriptionId, e.getMessage());
            return false;
        } catch (Exception e) {
            // Backstop: one malformed subscription must not fail the whole load nor keep the pod
            // not-ready. Skip it and continue with the rest.
            log.warn("Skipping subscription {} due to unexpected error: {}", subscriptionId, e.toString());
            registry.remove(subscriptionId);
            return false;
        }
    }

    public boolean isLoaded() {
        return loaded.get();
    }

    private RuntimeSubscription parse(String json) {
        try {
            return objectMapper.readValue(json, RuntimeSubscription.class);
        } catch (Exception e) {
            log.warn("Failed to parse runtime subscription config: {}", e.getMessage());
            return null;
        }
    }

    private String configKey(String subscriptionId) {
        return props.getRedis().getConfigKeyPrefix() + subscriptionId;
    }
}
