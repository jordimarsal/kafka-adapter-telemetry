package com.jordimarcal.telemetry.gateway.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jordimarcal.telemetry.contracts.TelemetryEvent;
import com.jordimarcal.telemetry.gateway.domain.TrafficProfile;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

class PublishTelemetryUseCaseTest {

    private final List<String> sent = new ArrayList<>();
    private final PublishTelemetryUseCase useCase =
            new PublishTelemetryUseCase((key, payload) -> sent.add(key + "|" + payload), new ObjectMapper(), ms -> {});

    @Test
    void simulateLowPublishesExactlyTwentyCleanEvents() {
        SimulationReport report = useCase.simulate(TrafficProfile.LOW);
        assertEquals(20, report.published());
        assertEquals(0, report.duplicates());
        assertEquals(0, report.corrupt());
        assertEquals(20, sent.size());
        assertTrue(report.durationMs() >= 0);
    }

    @Test
    void simulateOverloadCountsDuplicatesAndCorrupt() {
        SimulationReport report = useCase.simulate(TrafficProfile.OVERLOAD);
        assertEquals(1_940, report.published());
        assertEquals(40, report.duplicates());
        assertEquals(20, report.corrupt());
        assertEquals(2_000, sent.size());
    }

    @Test
    void publishSingleSendsKeyedJson() throws Exception {
        var event = TelemetryEvent.of(UUID.randomUUID(), "gateway-es-1", "ES", "UP", 120, Instant.now()).orElseThrow();
        useCase.publish(event);
        assertEquals(1, sent.size());
        String[] parts = sent.get(0).split("\\|", 2);
        assertEquals("gateway-es-1", parts[0]);
        var tree = new ObjectMapper().readTree(parts[1]);
        assertEquals("gateway-es-1", tree.get("adapterId").asString());
        assertEquals("UP", tree.get("status").asString());
    }
}
