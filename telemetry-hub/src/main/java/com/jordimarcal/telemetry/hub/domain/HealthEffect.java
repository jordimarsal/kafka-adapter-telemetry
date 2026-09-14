package com.jordimarcal.telemetry.hub.domain;

import com.jordimarcal.telemetry.contracts.AdapterId;
import com.jordimarcal.telemetry.contracts.AlertEvent;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Effect of telling an aggregate what happened: the new state to persist and,
 * at most, one alert to publish. Alerts fire only on the 0→active transition,
 * so replays and repeated DOWNs never raise a second one.
 */
public record HealthEffect(AdapterHealth next, Optional<AlertEvent> alertToPublish) {

    public HealthEffect {
        Objects.requireNonNull(next, "next is required");
        Objects.requireNonNull(alertToPublish, "alertToPublish is required");
    }
}
