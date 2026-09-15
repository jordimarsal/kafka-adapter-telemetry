package com.jordimarcal.telemetry.contracts;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * One telemetry observation reported by an adapter.
 * Construction is boundary-safe: {@link #of(...)} validates every field and
 * accumulates all failures instead of stopping at the first one.
 */
public record TelemetryEvent(
        UUID eventId,
        AdapterId adapterId,
        Country country,
        Status status,
        LatencyMs latencyMs,
        Instant occurredAt) {

    public static Result<TelemetryEvent, List<ValidationError>> of(
            UUID eventId, String adapterId, String country, String status, Integer latencyMs, Instant occurredAt) {

        List<ValidationError> errors = new ArrayList<>();

        UUID id = eventId;
        if (id == null) {
            errors.add(new ValidationError("eventId", "must be a non-null UUID"));
        }

        Instant at = occurredAt;
        if (at == null) {
            errors.add(new ValidationError("occurredAt", "must be an ISO-8601 instant"));
        }

        AdapterId adapter = AdapterId.parse(adapterId).fold(a -> a, e -> {
            errors.add(e);
            return null;
        });

        Country ctry = Country.parse(country).fold(c -> c, e -> {
            errors.add(e);
            return null;
        });

        Status st = parseStatus(status).fold(s -> s, e -> {
            errors.add(e);
            return null;
        });

        LatencyMs latency = LatencyMs.parse(latencyMs).fold(l -> l, e -> {
            errors.add(e);
            return null;
        });

        if (!errors.isEmpty()) {
            return Result.err(List.copyOf(errors));
        }
        return Result.ok(new TelemetryEvent(id, adapter, ctry, st, latency, at));
    }

    private static Result<Status, ValidationError> parseStatus(String raw) {
        if (raw == null) {
            return Result.err(new ValidationError("status", "must be one of UP, DEGRADED, DOWN"));
        }
        try {
            return Result.ok(Status.valueOf(raw.strip().toUpperCase(java.util.Locale.ROOT)));
        } catch (IllegalArgumentException _) {
            return Result.err(new ValidationError("status", "must be one of UP, DEGRADED, DOWN"));
        }
    }

    public boolean isHealthy() {
        return status == Status.UP;
    }

    public TelemetryEvent {
        Objects.requireNonNull(eventId, "eventId is required");
        Objects.requireNonNull(adapterId, "adapterId is required");
        Objects.requireNonNull(country, "country is required");
        Objects.requireNonNull(status, "status is required");
        Objects.requireNonNull(latencyMs, "latencyMs is required");
        Objects.requireNonNull(occurredAt, "occurredAt is required");
    }
}
