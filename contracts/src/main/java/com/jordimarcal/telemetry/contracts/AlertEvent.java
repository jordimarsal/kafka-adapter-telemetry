package com.jordimarcal.telemetry.contracts;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * An alert raised by the telemetry hub. The id is derived deterministically from
 * the event that triggered it, so replays of the same trigger never create a
 * second alert (idempotency by construction, not by discipline). The trigger
 * event id travels with the alert: it is the audit link between the alert and
 * the exact observation that crossed the threshold.
 */
public record AlertEvent(UUID alertId, AdapterId adapterId, String reason, Instant raisedAt, UUID triggerEventId) {

    public static AlertEvent forTrigger(UUID triggerEventId, AdapterId adapterId, String reason, Instant raisedAt) {
        Objects.requireNonNull(triggerEventId, "triggerEventId is required");
        UUID alertId = UUID.nameUUIDFromBytes(triggerEventId.toString().getBytes(StandardCharsets.UTF_8));
        return new AlertEvent(alertId, adapterId, reason, raisedAt, triggerEventId);
    }

    public AlertEvent {
        Objects.requireNonNull(alertId, "alertId is required");
        Objects.requireNonNull(adapterId, "adapterId is required");
        Objects.requireNonNull(reason, "reason is required");
        Objects.requireNonNull(raisedAt, "raisedAt is required");
        Objects.requireNonNull(triggerEventId, "triggerEventId is required");
    }
}
