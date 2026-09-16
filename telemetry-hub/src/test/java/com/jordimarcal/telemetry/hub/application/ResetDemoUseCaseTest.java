package com.jordimarcal.telemetry.hub.application;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.jordimarcal.telemetry.contracts.AdapterId;
import com.jordimarcal.telemetry.contracts.Status;
import com.jordimarcal.telemetry.contracts.TelemetryEvent;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ResetDemoUseCaseTest {

    private static final AdapterId GW = new AdapterId("gateway-es-1");
    private static final Instant T0 = Instant.parse("2026-09-14T10:00:00Z");

    @Test
    void afterResetTheSameEventIsProcessedAsNewNotAsDuplicate() {
        var store = new ProcessTelemetryUseCaseTest.InMemoryTelemetryStore();
        var health = new ProcessTelemetryUseCaseTest.InMemoryHealthRepository();
        var alerts = new ProcessTelemetryUseCaseTest.InMemoryAlerts();
        var tap = new ProcessTelemetryUseCaseTest.RecordingTap();
        var pipeline = new ProcessTelemetryUseCase(store, health, alerts, alerts, tap);
        var reset = new ResetDemoUseCase(store, alerts, health);

        var event = TelemetryEvent.of(UUID.randomUUID(), GW.value(), "ES", Status.UP.name(), 100, T0).orElseThrow();
        pipeline.process(event);
        assertEquals(1, tap.processed.size());

        reset.reset();
        pipeline.process(event);

        assertEquals(2, tap.processed.size(), "after a reset the same eventId must flow through as a new event");
        assertEquals(0, tap.duplicates.size(), "no event may be treated as a duplicate right after a reset");
    }

    @Test
    void afterResetAlertsCanRaiseAgainForTheSameTrigger() {
        var store = new ProcessTelemetryUseCaseTest.InMemoryTelemetryStore();
        var health = new ProcessTelemetryUseCaseTest.InMemoryHealthRepository();
        var alerts = new ProcessTelemetryUseCaseTest.InMemoryAlerts();
        var tap = new ProcessTelemetryUseCaseTest.RecordingTap();
        var pipeline = new ProcessTelemetryUseCase(store, health, alerts, alerts, tap);
        var reset = new ResetDemoUseCase(store, alerts, health);

        for (int i = 0; i < 3; i++) {
            pipeline.process(down());
        }
        assertEquals(1, tap.alerts.size());
        reset.reset();
        for (int i = 0; i < 3; i++) {
            pipeline.process(down());
        }

        assertEquals(2, tap.alerts.size(), "a fresh streak after a reset must raise its own alert");
    }

    private static TelemetryEvent down() {
        return TelemetryEvent.of(UUID.randomUUID(), GW.value(), "ES", Status.DOWN.name(), 100, T0).orElseThrow();
    }
}
