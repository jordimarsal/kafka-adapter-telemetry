package com.jordimarcal.telemetry.hub.domain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jordimarcal.telemetry.contracts.AdapterId;
import com.jordimarcal.telemetry.contracts.Status;
import com.jordimarcal.telemetry.contracts.TelemetryEvent;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class AdapterHealthTest {

    private static final AdapterId GW = new AdapterId("gateway-es-1");
    private static final Instant T0 = Instant.parse("2026-09-14T10:00:00Z");

    private static TelemetryEvent event(Status status, UUID eventId, Instant at) {
        return TelemetryEvent.of(eventId, GW.value(), "ES", status.name(), 100, at).orElseThrow();
    }

    @Test
    void initialHealthIsClean() {
        var health = AdapterHealth.initial(GW, T0);
        assertEquals(0, health.consecutiveDown());
        assertFalse(health.alertActive());
        assertEquals(T0, health.lastSeen());
    }

    @Test
    void twoDownsRaiseNoAlert_thirdDownRaisesExactlyOne() {
        var health = AdapterHealth.initial(GW, T0);
        UUID trigger = UUID.randomUUID();

        var e1 = health.observe(event(Status.DOWN, UUID.randomUUID(), T0.plusSeconds(1)));
        assertFalse(e1.alertToPublish().isPresent());
        health = e1.next();

        var e2 = health.observe(event(Status.DOWN, UUID.randomUUID(), T0.plusSeconds(2)));
        assertFalse(e2.alertToPublish().isPresent());
        health = e2.next();

        var e3 = health.observe(event(Status.DOWN, trigger, T0.plusSeconds(3)));
        Optional<com.jordimarcal.telemetry.contracts.AlertEvent> alert = e3.alertToPublish();
        assertTrue(alert.isPresent());
        assertEquals(3, e3.next().consecutiveDown());
        assertTrue(e3.next().alertActive());
        assertEquals("3 consecutive DOWN observations", alert.orElseThrow().reason());
        assertEquals(GW, alert.orElseThrow().adapterId());
    }

    @Test
    void fourthDownDoesNotRealert() {
        var health = downStreak(GW, 3);
        var effect = health.observe(event(Status.DOWN, UUID.randomUUID(), T0.plusSeconds(10)));
        assertFalse(effect.alertToPublish().isPresent());
        assertTrue(effect.next().alertActive());
        assertEquals(4, effect.next().consecutiveDown());
    }

    @Test
    void upResetsStreakAndClosesAlert() {
        var health = downStreak(GW, 3);
        var effect = health.observe(event(Status.UP, UUID.randomUUID(), T0.plusSeconds(10)));
        assertEquals(0, effect.next().consecutiveDown());
        assertFalse(effect.next().alertActive());
        assertFalse(effect.alertToPublish().isPresent());
    }

    @Test
    void degradedDoesNotTouchStreak() {
        var health = downStreak(GW, 2);
        var effect = health.observe(event(Status.DEGRADED, UUID.randomUUID(), T0.plusSeconds(10)));
        assertEquals(2, effect.next().consecutiveDown());
        assertFalse(effect.alertToPublish().isPresent());
    }

    @Test
    void alertIdIsDerivedFromTriggeringEvent() {
        UUID trigger = UUID.randomUUID();
        var effect = downStreak(GW, 2).observe(event(Status.DOWN, trigger, T0.plusSeconds(9)));
        var alert = effect.alertToPublish().orElseThrow();
        var again = com.jordimarcal.telemetry.contracts.AlertEvent.forTrigger(
                trigger, GW, "3 consecutive DOWN observations", T0.plusSeconds(9));
        assertEquals(again.alertId(), alert.alertId());
    }

    private static AdapterHealth downStreak(AdapterId id, int downs) {
        AdapterHealth health = AdapterHealth.initial(id, T0);
        for (int i = 1; i <= downs; i++) {
            health = health
                    .observe(event(Status.DOWN, UUID.randomUUID(), T0.plusSeconds(i)))
                    .next();
        }
        return health;
    }
}
