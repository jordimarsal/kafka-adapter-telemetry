package com.jordimarcal.telemetry.hub.infrastructure.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jordimarcal.telemetry.contracts.AlertEvent;
import com.jordimarcal.telemetry.contracts.AdapterId;
import com.jordimarcal.telemetry.contracts.Status;
import com.jordimarcal.telemetry.contracts.TelemetryEvent;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class InMemoryTelemetryMetricsTest {

    private static final Instant T0 = Instant.parse("2026-09-15T10:00:00Z");

    private static TelemetryEvent event(Status status) {
        return TelemetryEvent.of(UUID.randomUUID(), "gateway-es-1", "ES", status.name(), 100, T0).orElseThrow();
    }

    @Test
    void seqIsMonotonicAcrossAllFrameKinds() {
        var metrics = new InMemoryTelemetryMetrics();
        List<Long> seqs = new ArrayList<>();
        metrics.addListener(frame -> {
            switch (frame) {
                case InMemoryTelemetryMetrics.Frame.TelemetryFrame(var seq, _, _, _, _, _, _) -> seqs.add(seq);
                case InMemoryTelemetryMetrics.Frame.DuplicateFrame(var seq, _, _) -> seqs.add(seq);
                case InMemoryTelemetryMetrics.Frame.AlertFrame(var seq, _, _, _, _, _) -> seqs.add(seq);
                case InMemoryTelemetryMetrics.Frame.DltFrame(var seq, _) -> seqs.add(seq);
                case InMemoryTelemetryMetrics.Frame.HeartbeatFrame(var seq, _) -> seqs.add(seq);
            }
        });

        metrics.onProcessed(event(Status.UP));
        metrics.onDuplicate(event(Status.DOWN));
        metrics.onDlt("broken");
        metrics.onAlert(AlertEvent.forTrigger(UUID.randomUUID(), new AdapterId("gateway-es-1"), "3 DOWN", T0));
        metrics.heartbeat();

        assertEquals(List.of(1L, 2L, 3L, 4L, 5L), seqs);
    }

    @Test
    void totalsCountDuplicatesAlertsAndDlt() {
        var metrics = new InMemoryTelemetryMetrics();
        metrics.onDuplicate(event(Status.UP));
        metrics.onDuplicate(event(Status.UP));
        metrics.onAlert(AlertEvent.forTrigger(UUID.randomUUID(), new AdapterId("gateway-es-1"), "3 DOWN", T0));
        metrics.onDlt("broken");
        assertEquals(new InMemoryTelemetryMetrics.Totals(2, 1, 1), metrics.totals());
    }

    @Test
    void removedListenerStopsReceivingFrames() {
        var metrics = new InMemoryTelemetryMetrics();
        List<InMemoryTelemetryMetrics.Frame> received = new ArrayList<>();
        java.util.function.Consumer<InMemoryTelemetryMetrics.Frame> listener = received::add;
        metrics.addListener(listener);
        metrics.onProcessed(event(Status.UP));
        metrics.removeListener(listener);
        metrics.onProcessed(event(Status.UP));
        assertEquals(1, received.size());
    }

    @Test
    void aFailingListenerDoesNotStopTheOthers() {
        var metrics = new InMemoryTelemetryMetrics();
        List<InMemoryTelemetryMetrics.Frame> received = new ArrayList<>();
        metrics.addListener(_ -> {
            throw new IllegalStateException("broken listener");
        });
        metrics.addListener(received::add);
        metrics.onProcessed(event(Status.UP));
        assertEquals(1, received.size());
        assertTrue(metrics.seq() >= 1);
    }
}
