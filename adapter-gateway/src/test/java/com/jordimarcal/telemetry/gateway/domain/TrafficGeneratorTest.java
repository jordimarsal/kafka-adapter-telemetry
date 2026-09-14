package com.jordimarcal.telemetry.gateway.domain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import com.jordimarcal.telemetry.gateway.domain.PlannedMessage.Kind;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TrafficGeneratorTest {

    private static final Instant NOW = Instant.parse("2026-09-14T10:00:00Z");
    private static final ObjectMapper JSON = new ObjectMapper();

    private static long countKind(List<PlannedMessage> plan, Kind kind) {
        return plan.stream().filter(m -> m.kind() == kind).count();
    }

    @Test
    void sameSeedSamePlan() {
        var g1 = new TrafficGenerator(TrafficProfile.OVERLOAD, 42, List.of("a-1", "a-2"), e -> "{}");
        var g2 = new TrafficGenerator(TrafficProfile.OVERLOAD, 42, List.of("a-1", "a-2"), e -> "{}");
        assertEquals(g1.generate(NOW), g2.generate(NOW));
    }

    @Test
    void planSizeMatchesProfile() {
        var plan = new TrafficGenerator(TrafficProfile.LOW, 7, List.of("a-1"), e -> "{}").generate(NOW);
        assertEquals(20, plan.size());
    }

    @Test
    void overloadRatiosRespectedExactly() {
        var plan = new TrafficGenerator(TrafficProfile.OVERLOAD, 7, List.of("a-1"), e -> "{}").generate(NOW);
        assertEquals(2_000, plan.size());
        assertEquals(40, countKind(plan, Kind.DUPLICATE));
        assertEquals(20, countKind(plan, Kind.CORRUPT));
        assertEquals(1_940, countKind(plan, Kind.NEW));
    }

    @Test
    void corruptPayloadIsNotValidJson() {
        var plan = new TrafficGenerator(TrafficProfile.OVERLOAD, 3, List.of("a-1"), e -> "{}").generate(NOW);
        var mapper = new ObjectMapper();
        for (PlannedMessage m : plan.stream().filter(p -> p.kind() == Kind.CORRUPT).toList()) {
            assertThrows(JacksonException.class, () -> mapper.readTree(m.payloadJson()),
                    "corrupt payload must not be valid JSON: " + m.payloadJson());
        }
    }

    @Test
    void duplicatesRepeatAnEarlierEventId() {
        var plan = new TrafficGenerator(TrafficProfile.OVERLOAD, 11, List.of("a-1"), e -> "{}").generate(NOW);
        var seen = new java.util.HashSet<UUID>();
        for (PlannedMessage m : plan) {
            if (m.kind() == Kind.DUPLICATE) {
                assertTrue(seen.contains(m.eventId()), "duplicate must reuse a previously seen eventId");
            }
            seen.add(m.eventId());
        }
    }

    @Test
    void telemetryJsonRoundTrips() throws Exception {
        var plan = new TrafficGenerator(TrafficProfile.LOW, 9, List.of("a-1"), JSON::writeValueAsString).generate(NOW);
        var mapper = new ObjectMapper();
        for (PlannedMessage m : plan.stream().filter(p -> p.kind() == Kind.NEW).toList()) {
            var back = mapper.readValue(m.payloadJson(), com.jordimarcal.telemetry.contracts.TelemetryEvent.class);
            assertEquals(m.eventId(), back.eventId());
            assertEquals(m.key(), back.adapterId().value());
            assertTrue(!back.occurredAt().isBefore(NOW), "occurredAt must deserialize as an Instant");
        }
    }

    @Test
    void newPayloadsAreValidTelemetryJson() throws Exception {
        var plan = new TrafficGenerator(TrafficProfile.HIGH, 5, List.of("a-1", "a-2"), JSON::writeValueAsString)
                .generate(NOW);
        var mapper = new ObjectMapper();
        for (PlannedMessage m : plan.stream().filter(p -> p.kind() == Kind.NEW).toList()) {
            var tree = mapper.readTree(m.payloadJson());
            assertEquals(m.key(), tree.get("adapterId").asString());
            assertTrue(Set.of("UP", "DEGRADED", "DOWN").contains(tree.get("status").asString()));
        }
    }
}
