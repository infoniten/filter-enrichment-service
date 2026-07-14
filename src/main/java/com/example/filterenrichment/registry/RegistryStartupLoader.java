package com.example.filterenrichment.registry;

import com.example.filterenrichment.metamodel.MetamodelHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Startup sequence (§6): load the domain model from the Enrich Service, then load and compile the
 * runtime subscriptions from Redis. Best-effort: if a dependency is not yet reachable the pod stays
 * up but reports not-ready (§34) until it is.
 */
@Component
public class RegistryStartupLoader {

    private static final Logger log = LoggerFactory.getLogger(RegistryStartupLoader.class);

    private final MetamodelHolder metamodel;
    private final RuntimeConfigService runtimeConfigService;

    public RegistryStartupLoader(MetamodelHolder metamodel, RuntimeConfigService runtimeConfigService) {
        this.metamodel = metamodel;
        this.runtimeConfigService = runtimeConfigService;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        try {
            metamodel.load();
        } catch (Exception e) {
            log.error("Domain model load failed at startup; not-ready until Enrich Service is available", e);
            return;
        }
        try {
            runtimeConfigService.loadAll();
        } catch (Exception e) {
            log.error("Runtime subscription load failed at startup; not-ready until Redis is available", e);
        }
    }
}
