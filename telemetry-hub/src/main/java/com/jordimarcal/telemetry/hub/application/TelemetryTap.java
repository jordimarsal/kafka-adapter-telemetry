package com.jordimarcal.telemetry.hub.application;

import com.jordimarcal.telemetry.contracts.AlertEvent;
import com.jordimarcal.telemetry.contracts.TelemetryEvent;

/**
 * Outbound port: live tap on what the use case decides, for the metrics
 * stream. Implementers must be fast and non-throwing — the pipeline must
 * never wait on or fail because of observability.
 */
public interface TelemetryTap {

    void onProcessed(TelemetryEvent event);

    void onDuplicate(TelemetryEvent event);

    void onAlert(AlertEvent alert);
}
