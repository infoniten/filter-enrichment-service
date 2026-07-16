package com.example.filterenrichment.registry;

import com.example.filterenrichment.config.FilterEnrichmentProperties;
import com.example.filterenrichment.metamodel.MetamodelHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

/**
 * Startup sequence: load the domain model from the Enrich Service, then load and compile the runtime
 * subscriptions from Redis, and only then start the Kafka consumers.
 *
 * <p>The listener containers are created with {@code autoStartup=false}, so nothing is consumed until
 * this loader starts them. Loading is retried indefinitely at a configurable interval; while it has
 * not succeeded the pod stays not-ready (metadata / subscriptions / kafkaConsumer health are DOWN).
 * This guarantees no record is ever processed before the metamodel is available.
 */
@Component
public class RegistryStartupLoader {

    private static final Logger log = LoggerFactory.getLogger(RegistryStartupLoader.class);

    private final MetamodelHolder metamodel;
    private final RuntimeConfigService runtimeConfigService;
    private final KafkaListenerEndpointRegistry listenerRegistry;
    private final long retryIntervalMs;

    private volatile Thread loaderThread;

    public RegistryStartupLoader(MetamodelHolder metamodel,
                                 RuntimeConfigService runtimeConfigService,
                                 KafkaListenerEndpointRegistry listenerRegistry,
                                 FilterEnrichmentProperties props) {
        this.metamodel = metamodel;
        this.runtimeConfigService = runtimeConfigService;
        this.listenerRegistry = listenerRegistry;
        this.retryIntervalMs = props.getStartup().getRetryIntervalMs();
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        // Run off the main thread so a slow/unavailable upstream doesn't block application startup;
        // the pod is up (liveness) but not-ready until loading succeeds and consumers start.
        loaderThread = new Thread(this::loadThenStartConsumers, "startup-loader");
        loaderThread.setDaemon(true);
        loaderThread.start();
    }

    private void loadThenStartConsumers() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                metamodel.load();
                runtimeConfigService.loadAll();
                startConsumers();
                log.info("Startup complete: domain model + subscriptions loaded; Kafka consumers started");
                return;
            } catch (Exception e) {
                log.warn("Startup not complete ({}); retrying in {} ms — consumers remain stopped",
                        e.getMessage(), retryIntervalMs);
                try {
                    Thread.sleep(retryIntervalMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private void startConsumers() {
        for (MessageListenerContainer container : listenerRegistry.getListenerContainers()) {
            if (!container.isRunning()) {
                container.start();
            }
        }
    }
}
