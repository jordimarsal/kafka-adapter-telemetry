package com.jordimarcal.telemetry.hub.infrastructure.metrics;

import com.jordimarcal.telemetry.contracts.AlertEvent;
import com.jordimarcal.telemetry.contracts.TelemetryEvent;
import com.jordimarcal.telemetry.hub.application.TelemetryTap;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

/**
 * In-memory implementation of the {@link TelemetryTap} port: turns every
 * use-case decision into an immutable, monotonically sequenced {@link Frame}
 * and fans it out to stream listeners. Failing listeners are dropped from the
 * fan-out, never from the pipeline. All state is in-memory by design — the
 * snapshot endpoint restores cumulative context (spec §5.2).
 */
public final class InMemoryTelemetryMetrics implements TelemetryTap {

    public record Totals(long duplicates, long alerts, long dlt) {
    }

    public sealed interface Frame {
        record TelemetryFrame(long seq, UUID eventId, String adapterId, String status,
                int latencyMs, String country, Instant occurredAt) implements Frame {
        }

        record DuplicateFrame(long seq, UUID eventId, String adapterId) implements Frame {
        }

        record AlertFrame(long seq, UUID alertId, String adapterId, String reason,
                Instant raisedAt, UUID triggerEventId) implements Frame {
        }

        record DltFrame(long seq, String reason) implements Frame {
        }

        record HeartbeatFrame(long seq, Totals totals) implements Frame {
        }
    }

    private final AtomicLong seq = new AtomicLong();
    private final LongAdder duplicates = new LongAdder();
    private final LongAdder alerts = new LongAdder();
    private final LongAdder dlt = new LongAdder();
    private final List<Consumer<Frame>> listeners = new CopyOnWriteArrayList<>();

    public void addListener(Consumer<Frame> listener) {
        listeners.add(listener);
    }

    public void removeListener(Consumer<Frame> listener) {
        listeners.remove(listener);
    }

    public void onDlt(String reason) {
        dlt.increment();
        publish(new Frame.DltFrame(seq.incrementAndGet(), reason));
    }

    public Frame.HeartbeatFrame heartbeat() {
        return new Frame.HeartbeatFrame(seq.get(), totals());
    }

    public Totals totals() {
        return new Totals(duplicates.sum(), alerts.sum(), dlt.sum());
    }

    /**
     * Demo-reset only: zeroes the counters while {@code seq} keeps its
     * monotonic course, so clients never see a seq regression mid-stream.
     */
    public void reset() {
        duplicates.reset();
        alerts.reset();
        dlt.reset();
    }

    public long seq() {
        return seq.get();
    }

    @Override
    public void onProcessed(TelemetryEvent event) {
        publish(new Frame.TelemetryFrame(seq.incrementAndGet(), event.eventId(),
                event.adapterId().value(), event.status().name(), event.latencyMs().value(),
                event.country().code(), event.occurredAt()));
    }

    @Override
    public void onDuplicate(TelemetryEvent event) {
        duplicates.increment();
        publish(new Frame.DuplicateFrame(seq.incrementAndGet(), event.eventId(), event.adapterId().value()));
    }

    @Override
    public void onAlert(AlertEvent alert) {
        alerts.increment();
        publish(new Frame.AlertFrame(seq.incrementAndGet(), alert.alertId(), alert.adapterId().value(),
                alert.reason(), alert.raisedAt(), alert.triggerEventId()));
    }

    private void publish(Frame frame) {
        for (Consumer<Frame> listener : listeners) {
            try {
                listener.accept(frame);
            } catch (RuntimeException _) {
                listeners.remove(listener);
            }
        }
    }
}
