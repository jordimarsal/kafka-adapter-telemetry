package com.jordimarcal.telemetry.contracts;

import java.util.Objects;

/**
 * Observed latency of an adapter call, in milliseconds. Invariant: 0..60_000.
 * Behaviour, not bare data: {@link #isSlow()} encodes what "slow" means in this domain.
 */
public record LatencyMs(int value) {

    private static final int MAX = 60_000;
    private static final int SLOW_THRESHOLD = 1_000;

    public LatencyMs {
        Objects.requireNonNull(value, "latencyMs is required");
        if (value < 0 || value > MAX) {
            throw new IllegalArgumentException("latencyMs must be between 0 and " + MAX);
        }
    }

    public boolean isSlow() {
        return value >= SLOW_THRESHOLD;
    }

    public static Result<LatencyMs, ValidationError> parse(Integer raw) {
        try {
            return Result.ok(new LatencyMs(Objects.requireNonNull(raw, "latencyMs is required")));
        } catch (IllegalArgumentException | NullPointerException e) {
            return Result.err(new ValidationError("latencyMs", "must be an integer between 0 and " + MAX));
        }
    }
}
