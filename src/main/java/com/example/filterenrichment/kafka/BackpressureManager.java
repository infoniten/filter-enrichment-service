package com.example.filterenrichment.kafka;

import com.example.filterenrichment.config.FilterEnrichmentProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Bounds the number of in-flight enrichment requests (§31). A worker acquires a permit before
 * enriching; if none is available within the configured timeout the consumer partitions are paused
 * (stopping further polling) until capacity frees up, then resumed. This keeps memory bounded and
 * applies real backpressure to Kafka rather than accumulating records.
 */
@Component
public class BackpressureManager {

    private static final Logger log = LoggerFactory.getLogger(BackpressureManager.class);

    private final KafkaListenerEndpointRegistry registry;
    private final Semaphore permits;
    private final long acquireTimeoutMs;
    private final AtomicInteger inFlight = new AtomicInteger();

    public BackpressureManager(KafkaListenerEndpointRegistry registry, FilterEnrichmentProperties props) {
        this.registry = registry;
        this.permits = new Semaphore(props.getBackpressure().getMaxConcurrentHttpRequests());
        this.acquireTimeoutMs = props.getBackpressure().getAcquireTimeoutMs();
    }

    /** Runs {@code task} while holding one concurrency permit, pausing partitions if saturated. */
    public <T> T run(java.util.function.Supplier<T> task) {
        acquire();
        inFlight.incrementAndGet();
        try {
            return task.get();
        } finally {
            inFlight.decrementAndGet();
            permits.release();
            maybeResume();
        }
    }

    private void acquire() {
        try {
            if (permits.tryAcquire(acquireTimeoutMs, TimeUnit.MILLISECONDS)) {
                return;
            }
            log.warn("Enrich concurrency saturated; pausing consumption");
            pauseAll();
            permits.acquire(); // block until a permit frees up
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted acquiring backpressure permit", e);
        }
    }

    private void maybeResume() {
        // Resume once there is spare capacity again.
        if (permits.availablePermits() > 0) {
            resumeAll();
        }
    }

    private void pauseAll() {
        for (MessageListenerContainer c : registry.getListenerContainers()) {
            if (!c.isContainerPaused()) {
                c.pause();
            }
        }
    }

    private void resumeAll() {
        for (MessageListenerContainer c : registry.getListenerContainers()) {
            if (c.isPauseRequested()) {
                c.resume();
            }
        }
    }

    public int inFlight() {
        return inFlight.get();
    }

    public int pausedContainers() {
        int paused = 0;
        for (MessageListenerContainer c : registry.getListenerContainers()) {
            if (c.isContainerPaused()) {
                paused++;
            }
        }
        return paused;
    }
}
