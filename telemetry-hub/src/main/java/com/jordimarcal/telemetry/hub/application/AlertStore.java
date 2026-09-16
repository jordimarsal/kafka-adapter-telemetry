package com.jordimarcal.telemetry.hub.application;

import com.jordimarcal.telemetry.contracts.AlertEvent;

/**
 * Durable history of raised alerts. Implementations must tolerate replays:
 * recording the same alertId twice is a no-op, not an error.
 */
public interface AlertStore {

    void record(AlertEvent alert);

    /**
     * Demo-reset only: wipes the alert history so a new demo run raises its
     * own alerts. Never called from the ingest path.
     */
    void clear();
}
