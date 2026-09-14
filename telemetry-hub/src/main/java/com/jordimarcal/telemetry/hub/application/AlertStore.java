package com.jordimarcal.telemetry.hub.application;

import com.jordimarcal.telemetry.contracts.AlertEvent;

/**
 * Durable history of raised alerts. Implementations must tolerate replays:
 * recording the same alertId twice is a no-op, not an error.
 */
public interface AlertStore {

    void record(AlertEvent alert);
}
