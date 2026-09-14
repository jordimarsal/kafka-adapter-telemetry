package com.jordimarcal.telemetry.gateway.domain;

import java.util.UUID;

/**
 * One message the simulator must place on the wire.
 * CORRUPT messages carry a payload that is deliberately not valid JSON: they are
 * the poison pills that demonstrate the dead letter topic.
 */
public record PlannedMessage(String key, String payloadJson, UUID eventId, Kind kind) {

    public enum Kind {
        NEW,
        DUPLICATE,
        CORRUPT
    }
}
