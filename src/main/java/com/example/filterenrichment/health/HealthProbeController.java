package com.example.filterenrichment.health;

import org.springframework.boot.actuate.health.HealthComponent;
import org.springframework.boot.actuate.health.HealthEndpoint;
import org.springframework.boot.actuate.health.Status;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Exposes the spec's probe paths {@code /health/live} and {@code /health/ready} (§34), backed by the
 * actuator liveness / readiness health groups.
 */
@RestController
public class HealthProbeController {

    private final HealthEndpoint healthEndpoint;

    public HealthProbeController(HealthEndpoint healthEndpoint) {
        this.healthEndpoint = healthEndpoint;
    }

    @GetMapping("/health/live")
    public ResponseEntity<Map<String, String>> live() {
        return probe("liveness");
    }

    @GetMapping("/health/ready")
    public ResponseEntity<Map<String, String>> ready() {
        return probe("readiness");
    }

    private ResponseEntity<Map<String, String>> probe(String group) {
        HealthComponent component = healthEndpoint.healthForPath(group);
        Status status = component != null ? component.getStatus() : Status.UNKNOWN;
        HttpStatus http = Status.UP.equals(status) ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE;
        return ResponseEntity.status(http).body(Map.of("status", status.getCode()));
    }
}
