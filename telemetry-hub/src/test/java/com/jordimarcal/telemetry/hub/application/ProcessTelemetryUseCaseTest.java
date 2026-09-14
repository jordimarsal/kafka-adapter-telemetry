package com.jordimarcal.telemetry.hub.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jordimarcal.telemetry.contracts.AdapterId;
import com.jordimarcal.telemetry.contracts.AlertEvent;
import com.jordimarcal.telemetry.contracts.Result;
import com.jordimarcal.telemetry.contracts.Status;
import com.jordimarcal.telemetry.contracts.TelemetryEvent;
import com.jordimarcal.telemetry.hub.domain.AdapterHealth;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ProcessTelemetryUseCaseTest {

    private static final AdapterId GW = new AdapterId("gateway-es-1");
    private static final Instant T0 = Instant.parse("2026-09-14T10:00:00Z");

    static final class InMemoryTelemetryStore implements TelemetryStore {
        final Set<UUID> seen = new HashSet<>();
        int appended;

        @Override
        public Result<Long, DuplicateTelemetry> append(TelemetryEvent event) {
            if (!seen.add(event.eventId())) {
                return Result.err(new DuplicateTelemetry(event.eventId()));
            }
            return Result.ok((long) ++appended);
        }
    }

    static final class InMemoryHealthRepository implements HealthRepository {
        AdapterHealth lastSaved;
        int saves;

        @Override
        public AdapterHealth find(AdapterId adapterId) {
            return lastSaved != null && lastSaved.adapterId().equals(adapterId)
                    ? lastSaved
                    : AdapterHealth.initial(adapterId, Instant.EPOCH);
        }

        @Override
        public void save(AdapterHealth health) {
            lastSaved = health;
            saves++;
        }
    }

    static final class InMemoryAlerts implements AlertStore, AlertPublisher {
        final List<AlertEvent> recorded = new ArrayList<>();
        final List<AlertEvent> published = new ArrayList<>();

        @Override
        public void record(AlertEvent alert) {
            recorded.add(alert);
        }

        @Override
        public void publish(AlertEvent alert) {
            published.add(alert);
        }
    }

    private record Fakes(InMemoryTelemetryStore store, InMemoryHealthRepository health, InMemoryAlerts alerts,
                         ProcessTelemetryUseCase useCase) {
    }

    private static Fakes fakes() {
        var store = new InMemoryTelemetryStore();
        var health = new InMemoryHealthRepository();
        var alerts = new InMemoryAlerts();
        return new Fakes(store, health, alerts, new ProcessTelemetryUseCase(store, health, alerts, alerts));
    }

    private static TelemetryEvent event(Status status, UUID eventId) {
        return TelemetryEvent.of(eventId, GW.value(), "ES", status.name(), 100, T0).orElseThrow();
    }

    @Test
    void firstEventIsStoredAndHealthSavedWithoutAlert() {
        Fakes f = fakes();
        f.useCase().process(event(Status.UP, UUID.randomUUID()));
        assertEquals(1, f.store().appended);
        assertEquals(1, f.health().saves);
        assertEquals(0, f.health().lastSaved.consecutiveDown());
        assertTrue(f.alerts().published.isEmpty());
    }

    @Test
    void duplicateEventIsIgnoredEntirely() {
        Fakes f = fakes();
        UUID id = UUID.randomUUID();
        f.useCase().process(event(Status.UP, id));
        f.useCase().process(event(Status.UP, id));
        assertEquals(1, f.health().saves);
        assertEquals(1, f.store().appended);
    }

    @Test
    void threeDownsAlertExactlyOnce_fourthDownDoesNot() {
        Fakes f = fakes();
        f.useCase().process(event(Status.DOWN, UUID.randomUUID()));
        f.useCase().process(event(Status.DOWN, UUID.randomUUID()));
        f.useCase().process(event(Status.DOWN, UUID.randomUUID()));
        assertEquals(1, f.alerts().published.size(), "third DOWN must raise exactly one alert");
        f.useCase().process(event(Status.DOWN, UUID.randomUUID()));
        assertEquals(1, f.alerts().published.size(), "fourth DOWN must not raise a new alert");
        assertEquals(1, f.alerts().recorded.size());
        assertEquals(4, f.health().lastSaved.consecutiveDown(), "streak keeps counting while alert is active");
    }

    @Test
    void upClosesAlertSoNextStreakAlertsAgain() {
        Fakes f = fakes();
        for (int i = 0; i < 3; i++) {
            f.useCase().process(event(Status.DOWN, UUID.randomUUID()));
        }
        f.useCase().process(event(Status.UP, UUID.randomUUID()));
        assertEquals(1, f.alerts().published.size());
        for (int i = 0; i < 3; i++) {
            f.useCase().process(event(Status.DOWN, UUID.randomUUID()));
        }
        assertEquals(2, f.alerts().published.size(), "a new streak after UP must alert again");
    }
}
