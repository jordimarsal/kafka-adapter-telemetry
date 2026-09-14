package com.jordimarcal.telemetry.hub.application;

import com.jordimarcal.telemetry.contracts.Result;
import com.jordimarcal.telemetry.contracts.TelemetryEvent;
import java.util.UUID;

/**
 * Append-only store of raw telemetry. Idempotency lives here: the same
 * eventId appended twice is refused by the store, not remembered in memory.
 */
public interface TelemetryStore {

    Result<Long, DuplicateTelemetry> append(TelemetryEvent event);

    record DuplicateTelemetry(UUID eventId) {
    }
}
