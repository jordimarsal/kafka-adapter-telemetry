package com.jordimarcal.telemetry.gateway.domain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jordimarcal.telemetry.contracts.Result;
import org.junit.jupiter.api.Test;

class TrafficProfileTest {

    @Test
    void namedProfilesResolve() {
        for (String name : new String[] {"low", "moderate", "high", "overload"}) {
            assertTrue(TrafficProfile.named(name).isOk());
        }
        assertEquals("low", TrafficProfile.named("low").orElseThrow().name());
    }

    @Test
    void unknownProfileListsAvailableOnes() {
        Result<TrafficProfile, TrafficProfile.UnknownProfile> r = TrafficProfile.named("nope");
        TrafficProfile.UnknownProfile err = r.error();
        assertEquals("nope", err.requested());
        assertTrue(err.available().contains("low"));
        assertTrue(err.available().contains("overload"));
    }

    @Test
    void profilesAreCaseInsensitiveAndTrimmed() {
        assertTrue(TrafficProfile.named("  HIGH ").isOk());
    }

    @Test
    void overloadCarriesTheStressRatios() {
        TrafficProfile p = TrafficProfile.named("overload").orElseThrow();
        assertEquals(2_000, p.totalEvents());
        assertEquals(200, p.eventsPerSecond());
        assertEquals(0.02, p.duplicateRatio());
        assertEquals(0.01, p.corruptRatio());
    }
}
