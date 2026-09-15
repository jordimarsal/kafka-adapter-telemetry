package com.jordimarcal.telemetry.hub.infrastructure.readapi;

import com.jordimarcal.telemetry.hub.infrastructure.metrics.InMemoryTelemetryMetrics;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import tools.jackson.databind.json.JsonMapper;

/**
 * Read side of the metrics stream: snapshot for (re)connects, named SSE
 * events for the live feed and a keep-alive heartbeat. Pure forwarder —
 * aggregation happens in the client (spec §5.1).
 */
@RestController
public class MetricsStreamController {

    private static final Logger log = LoggerFactory.getLogger(MetricsStreamController.class);
    private static final long HEARTBEAT_SECONDS = 15;

    private final InMemoryTelemetryMetrics metrics;
    private final JsonMapper json = JsonMapper.builder().build();
    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();
    private final ScheduledExecutorService heartbeat = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().name("sse-heartbeat").daemon(true).factory());

    public MetricsStreamController(InMemoryTelemetryMetrics metrics) {
        this.metrics = metrics;
    }

    @PostConstruct
    void start() {
        metrics.addListener(this::forward);
        heartbeat.scheduleAtFixedRate(this::beat, HEARTBEAT_SECONDS, HEARTBEAT_SECONDS, TimeUnit.SECONDS);
    }

    @PreDestroy
    void stop() {
        heartbeat.shutdownNow();
        metrics.removeListener(this::forward);
        for (SseEmitter emitter : emitters) {
            emitter.complete();
        }
        emitters.clear();
    }

    @GetMapping(path = "/api/v1/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    SseEmitter stream() {
        SseEmitter emitter = new SseEmitter(0L);
        emitters.add(emitter);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError(_ -> emitters.remove(emitter));
        return emitter;
    }

    @GetMapping("/api/v1/metrics/snapshot")
    Snapshot snapshot() {
        return new Snapshot(metrics.seq(), metrics.totals());
    }

    record Snapshot(long seq, InMemoryTelemetryMetrics.Totals totals) {
    }

    private void beat() {
        forward(metrics.heartbeat());
    }

    private void forward(InMemoryTelemetryMetrics.Frame frame) {
        String event = switch (frame) {
            case InMemoryTelemetryMetrics.Frame.TelemetryFrame _ -> "telemetry";
            case InMemoryTelemetryMetrics.Frame.DuplicateFrame _ -> "duplicate";
            case InMemoryTelemetryMetrics.Frame.AlertFrame _ -> "alert";
            case InMemoryTelemetryMetrics.Frame.DltFrame _ -> "dlt";
            case InMemoryTelemetryMetrics.Frame.HeartbeatFrame _ -> "heartbeat";
        };
        String payload = json.writeValueAsString(frame);
        for (SseEmitter emitter : emitters) {
            try {
                synchronized (emitter) {
                    emitter.send(SseEmitter.event().name(event).data(payload, MediaType.APPLICATION_JSON));
                }
            } catch (IOException _) {
                emitters.remove(emitter);
            } catch (RuntimeException _) {
                emitters.remove(emitter);
            }
        }
    }
}
