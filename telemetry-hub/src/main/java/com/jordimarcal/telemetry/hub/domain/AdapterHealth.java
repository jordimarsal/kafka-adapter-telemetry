package com.jordimarcal.telemetry.hub.domain;

import com.jordimarcal.telemetry.contracts.AdapterId;
import com.jordimarcal.telemetry.contracts.AlertEvent;
import com.jordimarcal.telemetry.contracts.TelemetryEvent;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Current health of one adapter: how many DOWNs in a row, whether an alert is
 * already active, and when we last heard from it. Immutable: {@link #observe}
 * never mutates, it returns the effect of the observation (Tell, Don't Ask —
 * the aggregate decides, callers persist and publish what it answers).
 */
public record AdapterHealth(AdapterId adapterId, int consecutiveDown, boolean alertActive, Instant lastSeen) {

    private static final int ALERT_THRESHOLD = 3;
    private static final String ALERT_REASON = "3 consecutive DOWN observations";

    public AdapterHealth {
        Objects.requireNonNull(adapterId, "adapterId is required");
        Objects.requireNonNull(lastSeen, "lastSeen is required");
        if (consecutiveDown < 0) {
            throw new IllegalArgumentException("consecutiveDown must be >= 0");
        }
    }

    public static AdapterHealth initial(AdapterId adapterId, Instant now) {
        return new AdapterHealth(adapterId, 0, false, now);
    }

    public HealthEffect observe(TelemetryEvent event) {
        return switch (event.status()) {
            case UP -> new HealthEffect(new AdapterHealth(adapterId, 0, false, event.occurredAt()), Optional.empty());
            case DEGRADED -> new HealthEffect(
                    new AdapterHealth(adapterId, consecutiveDown, alertActive, event.occurredAt()), Optional.empty());
            case DOWN -> observeDown(event);
        };
    }

    private HealthEffect observeDown(TelemetryEvent event) {
        int streak = consecutiveDown + 1;
        if (streak >= ALERT_THRESHOLD && !alertActive) {
            AlertEvent alert = AlertEvent.forTrigger(event.eventId(), adapterId, ALERT_REASON, event.occurredAt());
            return new HealthEffect(new AdapterHealth(adapterId, streak, true, event.occurredAt()), Optional.of(alert));
        }
        return new HealthEffect(new AdapterHealth(adapterId, streak, alertActive, event.occurredAt()), Optional.empty());
    }
}
