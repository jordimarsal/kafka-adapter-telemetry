package com.jordimarcal.telemetry.contracts;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ValueObjectsTest {

    @Test
    void adapterIdAcceptsValidAndStrips() {
        assertEquals("gateway-es-1", AdapterId.parse(" gateway-es-1 ").orElseThrow().value());
    }

    @Test
    void adapterIdRejectsInvalidShapes() {
        for (String bad : new String[] {"AB", "GATEWAY", "with spaces", "a", null}) {
            assertTrue(AdapterId.parse(bad).isErr(), "expected err for: " + bad);
            assertEquals("adapterId", AdapterId.parse(bad).error().field());
        }
    }

    @Test
    void countryNormalizesCase() {
        assertEquals("ES", Country.parse("es").orElseThrow().code());
        assertEquals("UK", Country.parse("UK").orElseThrow().code());
    }

    @Test
    void countryOnlyFourCodesAllowed() {
        assertTrue(Country.parse("FR").isErr());
        assertTrue(Country.parse(null).isErr());
        assertEquals("country", Country.parse("FR").error().field());
    }

    @Test
    void latencyRange() {
        assertEquals(0, LatencyMs.parse(0).orElseThrow().value());
        assertTrue(LatencyMs.parse(-1).isErr());
        assertTrue(LatencyMs.parse(60_001).isErr());
        assertTrue(LatencyMs.parse(null).isErr());
        assertEquals("latencyMs", LatencyMs.parse(70_000).error().field());
    }

    @Test
    void latencySlowThreshold() {
        assertFalse(LatencyMs.parse(999).orElseThrow().isSlow());
        assertTrue(LatencyMs.parse(1_000).orElseThrow().isSlow());
    }
}
