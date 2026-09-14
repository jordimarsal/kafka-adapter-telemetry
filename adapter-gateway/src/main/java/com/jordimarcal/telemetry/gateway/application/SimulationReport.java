package com.jordimarcal.telemetry.gateway.application;

/**
 * What a simulation actually placed on the wire — the demo's honest receipt.
 */
public record SimulationReport(String profile, int published, int duplicates, int corrupt, long durationMs) {
}
