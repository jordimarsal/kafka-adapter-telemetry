package com.jordimarcal.telemetry.gateway.domain;

import com.jordimarcal.telemetry.contracts.AdapterId;
import com.jordimarcal.telemetry.contracts.Country;
import com.jordimarcal.telemetry.contracts.LatencyMs;
import com.jordimarcal.telemetry.contracts.Status;
import com.jordimarcal.telemetry.contracts.TelemetryEvent;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.function.Function;

import com.jordimarcal.telemetry.gateway.domain.PlannedMessage.Kind;

/**
 * Deterministic traffic planner: same profile + seed + adapters always produce
 * the same plan. Pure domain — serialization is injected, timing is not here.
 *
 * Ratio slots are deterministic (not sampled) so every run exercises exactly the
 * promised mix: corruption every 100th message, duplication every other 50th.
 */
public final class TrafficGenerator {

    private static final String CORRUPT_JSON = "{\"eventId\": \"not-json";
    private static final List<String> COUNTRIES = List.copyOf(Country.ALLOWED);

    private final TrafficProfile profile;
    private final long seed;
    private final List<String> adapterIds;
    private final Function<TelemetryEvent, String> serializer;

    public TrafficGenerator(
            TrafficProfile profile, long seed, List<String> adapterIds, Function<TelemetryEvent, String> serializer) {
        this.profile = Objects.requireNonNull(profile);
        this.adapterIds = List.copyOf(Objects.requireNonNull(adapterIds));
        this.serializer = Objects.requireNonNull(serializer);
        if (this.adapterIds.isEmpty()) {
            throw new IllegalArgumentException("at least one adapter id is required");
        }
        this.seed = seed;
    }

    public List<PlannedMessage> generate(Instant start) {
        SplittableRandom rng = new SplittableRandom(seed);
        long intervalMs = Math.max(1, 1_000 / profile.eventsPerSecond());
        List<PlannedMessage> plan = new ArrayList<>(profile.totalEvents());

        for (int i = 0; i < profile.totalEvents(); i++) {
            Instant at = start.plusMillis(i * intervalMs);
            switch (kindFor(i)) {
                case CORRUPT -> plan.add(new PlannedMessage(
                        randomAdapter(rng), CORRUPT_JSON, null, Kind.CORRUPT));
                case DUPLICATE -> {
                    PlannedMessage previous = plan.get(rng.nextInt(i));
                    plan.add(new PlannedMessage(previous.key(), previous.payloadJson(), previous.eventId(), Kind.DUPLICATE));
                }
                case NEW -> {
                    TelemetryEvent event = newEvent(rng, at);
                    plan.add(new PlannedMessage(event.adapterId().value(), serializer.apply(event), event.eventId(), Kind.NEW));
                }
            }
        }
        return List.copyOf(plan);
    }

    private Kind kindFor(int i) {
        if (profile.corruptRatio() > 0 && i % 100 == 99) {
            return Kind.CORRUPT;
        }
        if (profile.duplicateRatio() > 0 && i % 50 == 25) {
            return Kind.DUPLICATE;
        }
        return Kind.NEW;
    }

    private TelemetryEvent newEvent(SplittableRandom rng, Instant at) {
        double draw = rng.nextDouble();
        Status status = draw < profile.downRatio()
                ? Status.DOWN
                : draw < profile.downRatio() + profile.degradedRatio() ? Status.DEGRADED : Status.UP;
        int latency = switch (status) {
            case UP -> 20 + rng.nextInt(281);
            case DEGRADED -> 800 + rng.nextInt(1_201);
            case DOWN -> 3_000 + rng.nextInt(7_001);
        };
        UUID eventId = new UUID(rng.nextLong(), rng.nextLong());
        String country = COUNTRIES.get(rng.nextInt(COUNTRIES.size()));
        return TelemetryEvent.of(eventId, randomAdapter(rng), country, status.name(), latency, at).orElseThrow();
    }

    private String randomAdapter(SplittableRandom rng) {
        return adapterIds.get(rng.nextInt(adapterIds.size()));
    }
}
