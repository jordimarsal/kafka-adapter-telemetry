package com.jordimarcal.telemetry.gateway.domain;

import com.jordimarcal.telemetry.contracts.Result;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * A named traffic profile of the demo: how many events, at what pace, and with
 * which mix of degradation, duplication and corruption. A Value Object, not an if.
 */
public record TrafficProfile(
        String name,
        int totalEvents,
        int eventsPerSecond,
        double degradedRatio,
        double downRatio,
        double duplicateRatio,
        double corruptRatio) {

    public static final TrafficProfile LOW = new TrafficProfile("low", 20, 1, 0.0, 0.05, 0.0, 0.0);
    public static final TrafficProfile MODERATE = new TrafficProfile("moderate", 100, 10, 0.05, 0.0, 0.0, 0.0);
    public static final TrafficProfile HIGH = new TrafficProfile("high", 500, 50, 0.10, 0.15, 0.0, 0.0);
    public static final TrafficProfile OVERLOAD = new TrafficProfile("overload", 2_000, 200, 0.05, 0.05, 0.02, 0.01);

    private static final Map<String, TrafficProfile> NAMED = new LinkedHashMap<>(Map.of(
            LOW.name(), LOW,
            MODERATE.name(), MODERATE,
            HIGH.name(), HIGH,
            OVERLOAD.name(), OVERLOAD));

    public static Result<TrafficProfile, UnknownProfile> named(String raw) {
        if (raw == null) {
            return Result.err(new UnknownProfile(null, NAMED.keySet()));
        }
        TrafficProfile profile = NAMED.get(raw.strip().toLowerCase(Locale.ROOT));
        if (profile == null) {
            return Result.err(new UnknownProfile(raw, NAMED.keySet()));
        }
        return Result.ok(profile);
    }

    public static Set<String> availableNames() {
        return NAMED.keySet();
    }

    public TrafficProfile {
        if (totalEvents <= 0) {
            throw new IllegalArgumentException("totalEvents must be positive");
        }
        if (eventsPerSecond <= 0) {
            throw new IllegalArgumentException("eventsPerSecond must be positive");
        }
        if (degradedRatio < 0 || downRatio < 0 || degradedRatio + downRatio > 0.9) {
            throw new IllegalArgumentException("unhealthy ratio out of range");
        }
        if (duplicateRatio < 0 || duplicateRatio > 0.3 || corruptRatio < 0 || corruptRatio > 0.2) {
            throw new IllegalArgumentException("duplication/corruption ratio out of range");
        }
    }

    public record UnknownProfile(String requested, Set<String> available) {
    }
}
