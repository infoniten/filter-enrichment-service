package com.example.filterenrichment.health;

import com.example.filterenrichment.metamodel.MetamodelHolder;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/** Readiness contributor {@code metadata} (§34): UP once the domain model is loaded. */
@Component("metadata")
public class MetadataHealthIndicator implements HealthIndicator {

    private final MetamodelHolder metamodel;

    public MetadataHealthIndicator(MetamodelHolder metamodel) {
        this.metamodel = metamodel;
    }

    @Override
    public Health health() {
        if (!metamodel.isLoaded()) {
            return Health.down().withDetail("reason", "domain model not loaded").build();
        }
        return Health.up().withDetail("classes", metamodel.get().classCount()).build();
    }
}
