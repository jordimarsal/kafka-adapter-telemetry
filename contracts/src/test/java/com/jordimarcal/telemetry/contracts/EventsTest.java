package com.jordimarcal.telemetry.contracts;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class EventsTest {

    private static final Instant NOW = Instant.parse("2026-09-14T10:00:00Z");

    @Test
    void validEventAccumulatesNoErrors() {
        var r = TelemetryEvent.of(UUID.randomUUID(), "gateway-es-1", "ES", "UP", 120, NOW);
        assertTrue(r.isOk());
        assertTrue(r.orElseThrow().isHealthy());
    }

    @Test
    void invalidFieldsAccumulateErrors() {
        Result<TelemetryEvent, List<ValidationError>> r =
                TelemetryEvent.of(null, "BAD", "FR", "MAYBE", 99_999, NOW);
        List<ValidationError> errors = r.error();
        assertEquals(5, errors.size());
        assertTrue(errors.stream().anyMatch(e -> e.field().equals("eventId")));
        assertTrue(errors.stream().anyMatch(e -> e.field().equals("adapterId")));
        assertTrue(errors.stream().anyMatch(e -> e.field().equals("country")));
        assertTrue(errors.stream().anyMatch(e -> e.field().equals("status")));
        assertTrue(errors.stream().anyMatch(e -> e.field().equals("latencyMs")));
    }

    @Test
    void degradedAndDownAreNotHealthy() {
        assertFalse(TelemetryEvent.of(UUID.randomUUID(), "gw-1", "ES", "DEGRADED", 1_500, NOW).orElseThrow().isHealthy());
        assertFalse(TelemetryEvent.of(UUID.randomUUID(), "gw-1", "ES", "DOWN", 3_000, NOW).orElseThrow().isHealthy());
    }

    @Test
    void alertIdIsDeterministicPerTrigger() {
        UUID trigger = UUID.randomUUID();
        var a1 = AlertEvent.forTrigger(trigger, new AdapterId("gw-1"), "3 DOWN", NOW);
        var a2 = AlertEvent.forTrigger(trigger, new AdapterId("gw-1"), "3 DOWN", NOW);
        assertEquals(a1.alertId(), a2.alertId());
        assertNotEquals(a1.alertId(),
                AlertEvent.forTrigger(UUID.randomUUID(), new AdapterId("gw-1"), "3 DOWN", NOW).alertId());
    }

    @Test
    void topicNamesAreVersioned() {
        assertEquals("adapter.telemetry.v1", TopicNames.TELEMETRY);
        assertEquals("adapter.alerts.v1", TopicNames.ALERTS);
        assertEquals("adapter.telemetry.v1.dlt", TopicNames.TELEMETRY_DLT);
    }
}
