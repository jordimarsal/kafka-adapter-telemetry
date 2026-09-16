package com.jordimarcal.telemetry.hub.infrastructure.readapi;

import com.jordimarcal.telemetry.hub.application.ResetDemoUseCase;
import com.jordimarcal.telemetry.hub.infrastructure.metrics.InMemoryTelemetryMetrics;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Demo-reset endpoint: wipes persisted telemetry, alerts and health, zeroes
 * the in-memory counters and answers with the fresh snapshot. The stream's
 * {@code seq} keeps its monotonic course, so connected clients resync purely
 * through the returned totals.
 */
@RestController
public class DemoResetController {

    private final ResetDemoUseCase resetUseCase;
    private final InMemoryTelemetryMetrics metrics;

    public DemoResetController(ResetDemoUseCase resetUseCase, InMemoryTelemetryMetrics metrics) {
        this.resetUseCase = resetUseCase;
        this.metrics = metrics;
    }

    @PostMapping("/api/v1/demo/reset")
    MetricsStreamController.Snapshot reset() {
        resetUseCase.reset();
        metrics.reset();
        return new MetricsStreamController.Snapshot(metrics.seq(), metrics.totals());
    }
}
