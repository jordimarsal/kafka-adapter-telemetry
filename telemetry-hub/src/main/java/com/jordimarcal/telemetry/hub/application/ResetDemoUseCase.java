package com.jordimarcal.telemetry.hub.application;

/**
 * Demo-only reset: wipes persisted telemetry, alert history and adapter
 * health so the next demo run starts from zero — replayed event ids are
 * processed as new again. Never part of the ingest path.
 */
public class ResetDemoUseCase {

    private final TelemetryStore telemetryStore;
    private final AlertStore alertStore;
    private final HealthRepository healthRepository;

    public ResetDemoUseCase(TelemetryStore telemetryStore, AlertStore alertStore, HealthRepository healthRepository) {
        this.telemetryStore = telemetryStore;
        this.alertStore = alertStore;
        this.healthRepository = healthRepository;
    }

    public void reset() {
        telemetryStore.clear();
        alertStore.clear();
        healthRepository.clear();
    }
}
