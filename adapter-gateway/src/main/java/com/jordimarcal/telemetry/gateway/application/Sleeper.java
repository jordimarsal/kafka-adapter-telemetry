package com.jordimarcal.telemetry.gateway.application;

/**
 * Pacing seam for the simulator: production sleeps real milliseconds,
 * tests substitute a no-op. Unchecked by design — interruptions restore
 * the flag inside the infrastructure implementation.
 */
@FunctionalInterface
public interface Sleeper {

    void sleep(long millis);
}
