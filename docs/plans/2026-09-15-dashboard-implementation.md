# Live Demo Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the live Mission-Control dashboard for the demo: a `dashboard/` Vite+React app fed by a new hub SSE tap (`TelemetryTap` port → in-memory metrics → `GET /api/v1/stream`), with snapshot REST, DLT observer, gateway CORS and Terminal-Noir visuals.

**Architecture:** The hub use case notifies a new `TelemetryTap` port (same pattern as `AlertPublisher`); `InMemoryTelemetryMetrics` (infrastructure) turns notifications into sealed `Frame` records; `MetricsStreamController` broadcasts frames as named SSE events plus a snapshot endpoint and a 15 s heartbeat. A `DltObserver` Kafka listener counts dead letters into the same metrics. The React client folds frames client-side (no server aggregation) into ECharts series with rAF coalescing, and drives the demo by POSTing simulate profiles to the gateway (CORS-allowed for origin `:8082` only).

**Tech Stack:** Java 25 / Spring Boot 4.1 (existing — **zero new Maven dependencies**; SSE and Jackson 3 `tools.jackson` already on the classpath), Vite + React + TypeScript + Tailwind 4 + ECharts + framer-motion + Vitest/Testing Library (Node ≥ 22, present on this machine).

**Spec:** `docs/plans/2026-09-15-dashboard-design.md` — read it together with this plan; sections referenced below (§) come from it.

## Global Constraints

- Java 25 style everywhere: records, sealed interfaces, switch patterns with record deconstruction, `_` for unused bindings, `Math.clamp`, text blocks, no wildcard returns (Sonar S1452), SLF4J logging only (`log.error("msg", e)` shape), no `System.out`, no Lombok, no field injection, comments only where they explain intent (Sonar S1186 for empty overrides).
- **No new Maven dependencies in any module.** `contracts` stays dependency-free (Jackson annotations only). Frontend deps are exactly: `echarts`, `framer-motion`, `tailwindcss`, `@tailwindcss/vite` (build), `vitest`, `jsdom`, `@testing-library/*` (test). Nothing else without an ADR.
- `domain`/`application` packages must not import Spring or `tools.jackson`; the tap port is plain Java. Infrastructure implements it.
- Hub tests: plain JUnit for units (no Spring, no Mockito); `@WebMvcTest` + `@MockitoBean` only at the web edge; EmbeddedKafka ITs live in `TelemetryListenerIT` (the only `*IT` included by surefire — do not add new `*IT` files).
- DLT observer must never republish (no `DeadLetterPublishingRecoverer` on its error handler).
- Expected failures are `Result`, not exceptions; no silent `Err` (debug-log duplicates).
- Frontend: same-origin calls to the hub (`/api/v1/...`); gateway origin via `VITE_GATEWAY_URL` (default `http://localhost:8081`); Terminal Noir tokens exactly as spec §6 (`--bg #05070a`, `--panel #0a0e14`, `--line #1c2530`, `--text #e6edf3`, `--muted #8b98ab`, `--dim #4d5a6d`, `--up #3ddc97`, `--warn #ffb347`, `--down #ff5470`); respect `prefers-reduced-motion`.
- No `Thread.sleep` in tests — Awaitility (Java) / fake timers or awaited promises (Vitest).
- Commit after every task, repo style: `feat:`, `test:`, `docs:`, `chore:`.
- Build commands: `mvn -q package` (root), `npm --prefix dashboard test`, `npm --prefix dashboard run build`. Run hub from repo root so `file:dashboard/dist/` resolves.

---

### Task 1: `TelemetryTap` port and use-case notification

**Files:**
- Create: `telemetry-hub/src/main/java/com/jordimarcal/telemetry/hub/application/TelemetryTap.java`
- Modify: `telemetry-hub/src/main/java/com/jordimarcal/telemetry/hub/application/ProcessTelemetryUseCase.java`
- Modify: `telemetry-hub/src/main/java/com/jordimarcal/telemetry/hub/infrastructure/config/UseCaseConfig.java`
- Modify: `telemetry-hub/src/test/java/com/jordimarcal/telemetry/hub/application/ProcessTelemetryUseCaseTest.java`
- Modify: `telemetry-hub/src/test/java/com/jordimarcal/telemetry/hub/infrastructure/kafka/TelemetryListenerIT.java` (InMemoryConfig gains a no-op tap so the context still wires)

**Interfaces:**
- Consumes: existing `ProcessTelemetryUseCase(store, health, alerts, alerts)` constructor and its three decision points.
- Produces: `TelemetryTap` port with `void onProcessed(TelemetryEvent)`, `void onDuplicate(TelemetryEvent)`, `void onAlert(AlertEvent)`. `ProcessTelemetryUseCase` constructor becomes `(TelemetryStore, HealthRepository, AlertStore, AlertPublisher, TelemetryTap)`. Later tasks implement the port in `InMemoryTelemetryMetrics`.

- [ ] **Step 1: Write the failing tests** — add to `ProcessTelemetryUseCaseTest`:

```java
    static final class RecordingTap implements TelemetryTap {
        final List<TelemetryEvent> processed = new ArrayList<>();
        final List<TelemetryEvent> duplicates = new ArrayList<>();
        final List<AlertEvent> alerts = new ArrayList<>();

        @Override
        public void onProcessed(TelemetryEvent event) {
            processed.add(event);
        }

        @Override
        public void onDuplicate(TelemetryEvent event) {
            duplicates.add(event);
        }

        @Override
        public void onAlert(AlertEvent alert) {
            alerts.add(alert);
        }
    }
```

Extend `Fakes` with `RecordingTap tap` and build the use case with it:
`new ProcessTelemetryUseCase(store, health, alerts, alerts, tap)`. Add:

```java
    @Test
    void processedAndDuplicateEventsReachTheTap() {
        Fakes f = fakes();
        UUID id = UUID.randomUUID();
        f.useCase().process(event(Status.UP, id));
        f.useCase().process(event(Status.UP, id));
        assertEquals(1, f.tap().processed.size());
        assertEquals(1, f.tap().duplicates.size());
        assertEquals(id, f.tap().duplicates.getFirst().eventId());
    }

    @Test
    void raisedAlertsReachTheTap() {
        Fakes f = fakes();
        for (int i = 0; i < 3; i++) {
            f.useCase().process(event(Status.DOWN, UUID.randomUUID()));
        }
        assertEquals(3, f.tap().processed.size());
        assertEquals(1, f.tap().alerts.size());
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `mvn -q -pl telemetry-hub test -Dtest=ProcessTelemetryUseCaseTest`
Expected: COMPILATION ERROR — `TelemetryTap` does not exist / constructor arity mismatch.

- [ ] **Step 3: Implement** — create the port (plain Java, intent javadoc):

```java
package com.jordimarcal.telemetry.hub.application;

import com.jordimarcal.telemetry.contracts.AlertEvent;
import com.jordimarcal.telemetry.contracts.TelemetryEvent;

/**
 * Outbound port: live tap on what the use case decides, for the metrics
 * stream. Implementers must be fast and non-throwing — the pipeline must
 * never wait on or fail because of observability.
 */
public interface TelemetryTap {

    void onProcessed(TelemetryEvent event);

    void onDuplicate(TelemetryEvent event);

    void onAlert(AlertEvent alert);
}
```

In `ProcessTelemetryUseCase`: add `private final TelemetryTap tap;` as fifth constructor dependency; in `process`:
duplicate branch → `tap.onDuplicate(duplicate);` before `return;`; after `healthRepository.save(effect.next())` → `tap.onProcessed(event);`; inside `effect.alertToPublish().ifPresent(...)` after `alertPublisher.publish(alert)` → `tap.onAlert(alert);`.

In `UseCaseConfig.processTelemetryUseCase` add `TelemetryTap tap` parameter and pass it as fifth argument.

In `TelemetryListenerIT.InMemoryConfig` add (keeps the IT context wiring with a no-op until Task 2):

```java
        @Bean
        TelemetryTap telemetryTap() {
            // no-op tap: this wiring test asserts pipeline behaviour, not the metrics stream
            return new TelemetryTap() {
                @Override
                public void onProcessed(TelemetryEvent event) {
                }

                @Override
                public void onDuplicate(TelemetryEvent event) {
                }

                @Override
                public void onAlert(AlertEvent alert) {
                }
            };
        }
```

- [ ] **Step 4: Run tests until green** — `mvn -q -pl telemetry-hub test` (all hub tests).
- [ ] **Step 5: Commit**

```bash
git add telemetry-hub
git commit -m "feat: TelemetryTap port notifies the metrics stream from the use case"
```

---

### Task 2: `InMemoryTelemetryMetrics` — frames, counters, listeners

**Files:**
- Create: `telemetry-hub/src/main/java/com/jordimarcal/telemetry/hub/infrastructure/metrics/InMemoryTelemetryMetrics.java`
- Create: `telemetry-hub/src/main/java/com/jordimarcal/telemetry/hub/infrastructure/config/MetricsConfig.java`
- Create: `telemetry-hub/src/test/java/com/jordimarcal/telemetry/hub/infrastructure/metrics/InMemoryTelemetryMetricsTest.java`
- Modify: `telemetry-hub/src/test/java/com/jordimarcal/telemetry/hub/infrastructure/kafka/TelemetryListenerIT.java` (drop the no-op tap from Task 1 — `MetricsConfig` now provides the real bean)

**Interfaces:**
- Consumes: `TelemetryTap` (Task 1).
- Produces (used by Tasks 4–5 and the frontend contract in spec §5.1):

```java
public final class InMemoryTelemetryMetrics implements TelemetryTap {
    public record Totals(long duplicates, long alerts, long dlt) {}
    public sealed interface Frame {
        record TelemetryFrame(long seq, UUID eventId, String adapterId, String status,
                int latencyMs, String country, Instant occurredAt) implements Frame {}
        record DuplicateFrame(long seq, UUID eventId, String adapterId) implements Frame {}
        record AlertFrame(long seq, UUID alertId, String adapterId, String reason,
                Instant raisedAt, UUID triggerEventId) implements Frame {}
        record DltFrame(long seq, String reason) implements Frame {}
        record HeartbeatFrame(long seq, Totals totals) implements Frame {}
    }
    public void addListener(Consumer<Frame> listener)
    public void removeListener(Consumer<Frame> listener)
    public void onDlt(String reason)      // called by DltObserver (Task 5)
    public Totals totals()                // live counters
    public long seq()                     // monotonic across all frames
}
```

- [ ] **Step 1: Write the failing test** (plain JUnit):

```java
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
                case InMemoryTelemetryMetrics.TelemetryFrame(var seq, _, _, _, _, _, _) -> seqs.add(seq);
                case InMemoryTelemetryMetrics.DuplicateFrame(var seq, _, _) -> seqs.add(seq);
                case InMemoryTelemetryMetrics.AlertFrame(var seq, _, _, _, _, _) -> seqs.add(seq);
                case InMemoryTelemetryMetrics.DltFrame(var seq, _) -> seqs.add(seq);
                case InMemoryTelemetryMetrics.HeartbeatFrame(var seq, _) -> seqs.add(seq);
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
```

- [ ] **Step 2: Run to verify failure** — `mvn -q -pl telemetry-hub test -Dtest=InMemoryTelemetryMetricsTest` → COMPILATION ERROR (class missing).
- [ ] **Step 3: Implement** `InMemoryTelemetryMetrics`:

```java
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
```

Create `MetricsConfig` (composition root, UseCaseConfig pattern):

```java
package com.jordimarcal.telemetry.hub.infrastructure.config;

import com.jordimarcal.telemetry.hub.infrastructure.metrics.InMemoryTelemetryMetrics;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Composition root of the metrics stream: one shared in-memory registry. */
@Configuration
public class MetricsConfig {

    @Bean
    InMemoryTelemetryMetrics inMemoryTelemetryMetrics() {
        return new InMemoryTelemetryMetrics();
    }
}
```

In `TelemetryListenerIT.InMemoryConfig`: **delete** the no-op `TelemetryTap` bean from Task 1 — the real bean now comes from `MetricsConfig` (the `config` package is not excluded by `InMemoryStoresOnly`). The test import of `TelemetryEvent` stays (other tests use it).

- [ ] **Step 4: Run until green** — `mvn -q -pl telemetry-hub test` (unit test + IT context wiring both pass).
- [ ] **Step 5: Commit**

```bash
git add telemetry-hub
git commit -m "feat: in-memory metrics registry turns TelemetryTap calls into sequenced frames"
```

---

### Task 3: `MetricsStreamController` — snapshot endpoint

**Files:**
- Create: `telemetry-hub/src/main/java/com/jordimarcal/telemetry/hub/infrastructure/readapi/MetricsStreamController.java`
- Create: `telemetry-hub/src/test/java/com/jordimarcal/telemetry/hub/infrastructure/readapi/MetricsStreamControllerTest.java`

**Interfaces:**
- Consumes: `InMemoryTelemetryMetrics` (Task 2).
- Produces: `GET /api/v1/metrics/snapshot` → `{"seq": 42, "totals": {"duplicates": 95, "alerts": 14, "dlt": 20}}` (spec §5.2); `GET /api/v1/stream` (implemented in Task 4, same class); heartbeat frames every 15 s.

- [ ] **Step 1: Write the failing test** (`@WebMvcTest` slice, Mockito only at this edge):

```java
package com.jordimarcal.telemetry.hub.infrastructure.readapi;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.jordimarcal.telemetry.hub.infrastructure.metrics.InMemoryTelemetryMetrics;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(MetricsStreamController.class)
class MetricsStreamControllerTest {

    @Autowired
    private MockMvc mvc;

    @MockitoBean
    private InMemoryTelemetryMetrics metrics;

    @Test
    void snapshotReturnsSeqAndTotals() throws Exception {
        when(metrics.seq()).thenReturn(42L);
        when(metrics.totals()).thenReturn(new InMemoryTelemetryMetrics.Totals(95, 14, 20));

        mvc.perform(get("/api/v1/metrics/snapshot"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.seq").value(42))
                .andExpect(jsonPath("$.totals.duplicates").value(95))
                .andExpect(jsonPath("$.totals.alerts").value(14))
                .andExpect(jsonPath("$.totals.dlt").value(20));
    }
}
```

- [ ] **Step 2: Run to verify failure** — `mvn -q -pl telemetry-hub test -Dtest=MetricsStreamControllerTest` → 404 (controller missing).
- [ ] **Step 3: Implement** the controller (snapshot + SSE + heartbeat; the SSE part is covered in Task 4):

```java
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
            case InMemoryTelemetryMetrics.TelemetryFrame _ -> "telemetry";
            case InMemoryTelemetryMetrics.DuplicateFrame _ -> "duplicate";
            case InMemoryTelemetryMetrics.AlertFrame _ -> "alert";
            case InMemoryTelemetryMetrics.DltFrame _ -> "dlt";
            case InMemoryTelemetryMetrics.HeartbeatFrame _ -> "heartbeat";
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
```

- [ ] **Step 4: Run until green** — `mvn -q -pl telemetry-hub test -Dtest=MetricsStreamControllerTest`.
- [ ] **Step 5: Commit**

```bash
git add telemetry-hub
git commit -m "feat: metrics snapshot endpoint and SSE stream controller"
```

---

### Task 4: SSE delivery test — frames reach the client

**Files:**
- Modify: `telemetry-hub/src/test/java/com/jordimarcal/telemetry/hub/infrastructure/readapi/MetricsStreamControllerTest.java`

**Interfaces:**
- Consumes: `MetricsStreamController` (Task 3), metrics listener contract (Task 2).
- Produces: proof that a `Frame` published by the metrics registry reaches an SSE client as `event:telemetry\ndata:{...}` — the contract the React client (Task 8) codes against.

- [ ] **Step 1: Write the failing test** — add to `MetricsStreamControllerTest`:

```java
    @Test
    void streamDeliversMetricsFramesAsNamedServerSentEvents() throws Exception {
        AtomicReference<Consumer<InMemoryTelemetryMetrics.Frame>> listener = new AtomicReference<>();
        doAnswer(invocation -> {
            listener.set(invocation.getArgument(0));
            return null;
        }).when(metrics).addListener(any());

        MvcResult result = mvc.perform(get("/api/v1/stream"))
                .andExpect(request().asyncStarted())
                .andReturn();

        listener.get().accept(new InMemoryTelemetryMetrics.TelemetryFrame(
                7, UUID.randomUUID(), "gateway-es-1", "UP", 120, "ES", Instant.parse("2026-09-15T10:00:00Z")));

        MvcResult dispatched = mvc.perform(asyncDispatch(result))
                .andReturn();

        String body = dispatched.getResponse().getContentAsString();
        assertTrue(body.contains("event:telemetry"));
        assertTrue(body.contains("gateway-es-1"));
        assertFalse(body.contains("event:heartbeat"));
    }
```

New imports: `static org.mockito.Mockito.any, doAnswer`; `static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*` already partially there — add `asyncDispatch` from `org.springframework.test.web.servlet`; `MvcResult`, `AtomicReference`, `Consumer`, `Instant`, `UUID`, `assertFalse`, `assertTrue`.

- [ ] **Step 2: Run — expected GREEN already** (implementation from Task 3 covers it). If red, fix the controller, not the test. This step is the contract pin: it fails if anyone renames events or drops the fan-out.
- [ ] **Step 3: Commit**

```bash
git add telemetry-hub
git commit -m "test: pin SSE wire contract of the metrics stream"
```

---

### Task 5: `DltObserver` — count dead letters into the metrics

**Files:**
- Create: `telemetry-hub/src/main/java/com/jordimarcal/telemetry/hub/infrastructure/metrics/DltObserver.java`
- Modify: `telemetry-hub/src/main/java/com/jordimarcal/telemetry/hub/infrastructure/config/KafkaConsumerConfig.java` (add `dltListenerContainerFactory`)
- Modify: `telemetry-hub/src/test/java/com/jordimarcal/telemetry/hub/infrastructure/kafka/TelemetryListenerIT.java` (add counting test)

**Interfaces:**
- Consumes: `InMemoryTelemetryMetrics.onDlt(String)` (Task 2), `TopicNames.TELEMETRY_DLT`.
- Produces: DLT messages increment `totals().dlt()` and emit `DltFrame`s → the frontend integrity panel. The observer's own error handler must never republish (no loop).

- [ ] **Step 1: Write the failing test** — add to `TelemetryListenerIT`:

```java
    @Autowired
    private InMemoryTelemetryMetrics metrics;

    @Test
    void dltObserverCountsMessagesLandingOnTheDlt() {
        producer().send(TopicNames.TELEMETRY, "it-dlt-count", "{\"trencat");

        Awaitility.await().atMost(TIMEOUT).untilAsserted(() ->
                assertTrue(metrics.totals().dlt() >= 1, "dlt observer must count the dead letter"));
    }
```

Import `com.jordimarcal.telemetry.hub.infrastructure.metrics.InMemoryTelemetryMetrics`. Note: `malformedJsonIsRoutedToTheDlt` (existing) proves the DLT hop itself; this test proves the observer counts it.

- [ ] **Step 2: Run to verify failure** — `mvn -q -pl telemetry-hub test -Dtest=TelemetryListenerIT` → new test times out at 5 s (`dlt` stays 0).
- [ ] **Step 3: Implement** — in `KafkaConsumerConfig` add the factory (own String deserializers, own error handler **without** recoverer; add `Logger` and imports `org.apache.kafka.clients.consumer.ConsumerConfig`, `...serialization.StringDeserializer`, `ConcurrentKafkaListenerContainerFactory`, `DefaultKafkaConsumerFactory`, `ConsumerRecord` not needed here):

```java
    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> dltListenerContainerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "telemetry-hub-dlt",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(props));
        // no DeadLetterPublishingRecoverer here: an observer that republishes to its own topic loops forever
        factory.setCommonErrorHandler(new DefaultErrorHandler(
                (record, _) -> log.warn("dlt observer gave up on key={}", record.key()),
                new FixedBackOff(500, 2)));
        return factory;
    }
```

Create `DltObserver`:

```java
package com.jordimarcal.telemetry.hub.infrastructure.metrics;

import com.jordimarcal.telemetry.contracts.TopicNames;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Watches the DLT topic so the dashboard can show dead letters in real time.
 * Counts only — recovery stays a manual, deliberate act on the raw topic.
 */
@Component
public class DltObserver {

    private static final Logger log = LoggerFactory.getLogger(DltObserver.class);
    private static final int REASON_MAX = 120;

    private final InMemoryTelemetryMetrics metrics;

    DltObserver(InMemoryTelemetryMetrics metrics) {
        this.metrics = metrics;
    }

    @KafkaListener(id = "dlt-observer", topics = TopicNames.TELEMETRY_DLT,
            containerFactory = "dltListenerContainerFactory")
    public void observe(ConsumerRecord<String, String> record) {
        String value = record.value();
        String reason = value == null ? "unparseable"
                : value.substring(0, Math.clamp(value.length(), 0, REASON_MAX));
        log.debug("dlt message key={}", record.key());
        metrics.onDlt(reason);
    }
}
```

- [ ] **Step 4: Run until green** — `mvn -q -pl telemetry-hub test` (whole module: the new IT test passes with the embedded broker; slice tests unaffected).
- [ ] **Step 5: Commit**

```bash
git add telemetry-hub
git commit -m "feat: DltObserver counts dead letters into the metrics stream"
```

---

### Task 6: Gateway CORS for the dashboard origin

**Files:**
- Create: `adapter-gateway/src/main/java/com/jordimarcal/telemetry/gateway/infrastructure/config/CorsConfig.java`
- Create: `adapter-gateway/src/test/java/com/jordimarcal/telemetry/gateway/infrastructure/config/CorsConfigTest.java`

**Interfaces:**
- Consumes: nothing (pure web config).
- Produces: preflight/actual CORS allowance for `POST /api/v1/telemetry/**` and `POST /api/v1/telemetry/simulate` from exactly `http://localhost:8082`; other origins rejected. The frontend `simulate()` (Task 11) depends on this.

- [ ] **Step 1: Write the failing test**:

```java
package com.jordimarcal.telemetry.gateway.infrastructure.config;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.options;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.jordimarcal.telemetry.gateway.application.PublishTelemetryUseCase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(TelemetryController.class)
@Import(CorsConfig.class)
class CorsConfigTest {

    @Autowired
    private MockMvc mvc;

    @MockitoBean
    private PublishTelemetryUseCase useCase;

    @Test
    void preflightFromTheDashboardOriginIsAllowed() throws Exception {
        mvc.perform(options("/api/v1/telemetry")
                        .header(HttpHeaders.ORIGIN, "http://localhost:8082")
                        .header(HttpHeaders.ACCESS_CONTROL_REQUEST_METHOD, "POST"))
                .andExpect(status().isOk())
                .andExpect(header().string(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, "http://localhost:8082"));
    }

    @Test
    void preflightFromAnyOtherOriginIsRejected() throws Exception {
        mvc.perform(options("/api/v1/telemetry")
                        .header(HttpHeaders.ORIGIN, "http://evil.example")
                        .header(HttpHeaders.ACCESS_CONTROL_REQUEST_METHOD, "POST"))
                .andExpect(status().isForbidden());
    }
}
```

- [ ] **Step 2: Run to verify failure** — `mvn -q -pl adapter-gateway test -Dtest=CorsConfigTest` → first test fails (no `Access-Control-Allow-Origin`).
- [ ] **Step 3: Implement**:

```java
package com.jordimarcal.telemetry.gateway.infrastructure.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * The only cross-origin caller of the gateway is the dashboard served by the
 * telemetry-hub on :8082. Anything else stays browser-blocked.
 */
@Configuration
public class CorsConfig implements WebMvcConfigurer {

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/api/v1/**")
                .allowedOrigins("http://localhost:8082")
                .allowedMethods("GET", "POST")
                .maxAge(3600);
    }
}
```

- [ ] **Step 4: Run until green** — `mvn -q -pl adapter-gateway test`, then `mvn -q package` (full reactor).
- [ ] **Step 5: Commit**

```bash
git add adapter-gateway
git commit -m "feat: gateway allows the dashboard origin for demo control calls"
```

---

### Task 7: Frontend scaffold (`dashboard/`)

**Files:**
- Create: `dashboard/` (Vite react-ts template), `dashboard/vite.config.ts`, `dashboard/src/test/setup.ts`, `dashboard/src/App.tsx`, `dashboard/src/index.css`
- Modify: `.gitignore`

**Interfaces:**
- Produces: `npm --prefix dashboard test` (Vitest) and `npm --prefix dashboard run build` working; dev proxy `/api → :8082`; the empty `App` shell the next tasks fill. `VITE_GATEWAY_URL` env is the gateway base (Task 12).

- [ ] **Step 1: Scaffold and install** (Node 22 is present):

```bash
npm create vite@latest dashboard -- --template react-ts
npm --prefix dashboard install
npm --prefix dashboard install echarts framer-motion
npm --prefix dashboard install -D tailwindcss @tailwindcss/vite vitest jsdom @testing-library/react @testing-library/user-event @testing-library/jest-dom
rm dashboard/src/App.css dashboard/src/assets/react.svg
```

- [ ] **Step 2: Configure** — replace `dashboard/vite.config.ts`:

```ts
/// <reference types="vitest/config" />
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    proxy: { '/api': 'http://localhost:8082' },
  },
  test: {
    environment: 'jsdom',
    setupFiles: './src/test/setup.ts',
  },
})
```

Create `dashboard/src/test/setup.ts`:

```ts
import '@testing-library/jest-dom/vitest'
```

Add `"test": "vitest run"` and `"test:watch": "vitest"` to `scripts` in `dashboard/package.json`. Replace `dashboard/src/index.css` with `@import "tailwindcss";`. Write the minimal shell `dashboard/src/App.tsx`:

```tsx
export default function App() {
  return <div className="min-h-screen bg-bg p-4 font-mono text-fg">ADAPTER TELEMETRY</div>
}
```

Append to `.gitignore`:

```
dashboard/node_modules/
dashboard/dist/
```

- [ ] **Step 3: Write the failing smoke test** — `dashboard/src/App.test.tsx`:

```tsx
import { render, screen } from '@testing-library/react'
import { expect, test } from 'vitest'
import App from './App'

test('renders the dashboard shell', () => {
  render(<App />)
  expect(screen.getByText('ADAPTER TELEMETRY')).toBeInTheDocument()
})
```

- [ ] **Step 4: Run** — `npm --prefix dashboard test` → PASS; `npm --prefix dashboard run build` → builds `dashboard/dist/`.
- [ ] **Step 5: Commit**

```bash
git add dashboard .gitignore
git commit -m "feat: dashboard scaffold (vite+react+tailwind+vitest, proxied to the hub)"
```

---

### Task 8: Terminal Noir theme and Mission-Control shell

**Files:**
- Modify: `dashboard/src/index.css`
- Create: `dashboard/src/components/Panel.tsx`
- Modify: `dashboard/src/App.tsx`, `dashboard/src/App.test.tsx`

**Interfaces:**
- Produces: Tailwind utilities `bg-bg/panel/line`, `text-fg/muted/dim/up/warn/down` (from `@theme`), the `Panel` primitive (`{label, children}`) used by every panel in Tasks 12–15, and the spec §6 grid skeleton.

- [ ] **Step 1: Write the failing test** — extend `App.test.tsx` to assert the shell regions:

```tsx
test('renders the mission control grid regions', () => {
  render(<App />)
  expect(screen.getByText('throughput · ev/s')).toBeInTheDocument()
  expect(screen.getByText('latency · ms')).toBeInTheDocument()
  expect(screen.getByText('pipeline')).toBeInTheDocument()
  expect(screen.getByText(/alerts ·/)).toBeInTheDocument()
  expect(screen.getByText('integrity')).toBeInTheDocument()
  expect(screen.getByText(/adapters ·/)).toBeInTheDocument()
  expect(screen.getByText('demo control')).toBeInTheDocument()
})
```

- [ ] **Step 2: Run to verify failure** — `npm --prefix dashboard test` → cannot find region labels.
- [ ] **Step 3: Implement** — `index.css` (spec §6 tokens):

```css
@import "tailwindcss";

@theme {
  --color-bg: #05070a;
  --color-panel: #0a0e14;
  --color-line: #1c2530;
  --color-fg: #e6edf3;
  --color-muted: #8b98ab;
  --color-dim: #4d5a6d;
  --color-up: #3ddc97;
  --color-warn: #ffb347;
  --color-down: #ff5470;
  --font-mono: ui-monospace, "JetBrains Mono", "Cascadia Mono", Menlo, monospace;
}

html, body {
  background: var(--color-bg);
}

.num {
  font-size: clamp(1.5rem, 3vw, 2.25rem);
  font-weight: 700;
  letter-spacing: -0.02em;
  color: var(--color-fg);
  text-shadow: 0 0 18px rgb(61 220 151 / 0.35);
}

.cap {
  font-size: 8px;
  text-transform: uppercase;
  letter-spacing: 0.2em;
  color: var(--color-dim);
}

@keyframes flow {
  to { stroke-dashoffset: -20; }
}

.flow-line {
  animation: flow 1s linear infinite;
}

@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after {
    animation-duration: 0.01ms !important;
    transition-duration: 0.01ms !important;
  }
}
```

`dashboard/src/components/Panel.tsx`:

```tsx
import type { ReactNode } from 'react'

export function Panel({ label, children }: { label: string; children: ReactNode }) {
  return (
    <section className="flex min-h-0 flex-col rounded-lg border border-line bg-panel p-3">
      <h2 className="cap mb-2">{label}</h2>
      <div className="min-h-0 flex-1">{children}</div>
    </section>
  )
}
```

`App.tsx` shell with placeholder panels (each later task replaces its own):

```tsx
import { Panel } from './components/Panel'

export default function App() {
  return (
    <div className="min-h-screen bg-bg p-4 font-mono text-fg">
      <div className="grid grid-cols-[1fr_2fr_1fr] grid-rows-[auto_minmax(0,1fr)_auto_auto] gap-3">
        <div className="col-span-3 rounded-lg border border-line bg-panel px-4 py-2 text-xs">
          ADAPTER TELEMETRY
        </div>
        <Panel label="pipeline">—</Panel>
        <div className="grid min-h-0 grid-rows-2 gap-3">
          <Panel label="throughput · ev/s">—</Panel>
          <Panel label="latency · ms">—</Panel>
        </div>
        <div className="grid min-h-0 grid-rows-2 gap-3">
          <Panel label="alerts · 0">—</Panel>
          <Panel label="integrity">—</Panel>
        </div>
        <div className="col-span-3">
          <Panel label="adapters · 0">—</Panel>
        </div>
        <div className="col-span-3">
          <Panel label="demo control">—</Panel>
        </div>
      </div>
    </div>
  )
}
```

- [ ] **Step 4: Run** — `npm --prefix dashboard test` → PASS; `npm --prefix dashboard run build` → OK.
- [ ] **Step 5: Commit**

```bash
git add dashboard
git commit -m "feat: terminal-noir theme and mission-control grid shell"
```

---

### Task 9: Stream types and SSE client

**Files:**
- Create: `dashboard/src/stream/types.ts`, `dashboard/src/stream/sseClient.ts`, `dashboard/src/stream/sseClient.test.ts`

**Interfaces:**
- Consumes: hub wire contract (spec §5.1): named events `telemetry|duplicate|alert|dlt|heartbeat`, JSON payload without `kind` (the SSE event name carries the kind).
- Produces (used by store, lifecycle, demo machine):

```ts
export type Status = 'UP' | 'DEGRADED' | 'DOWN'
export interface Totals { duplicates: number; alerts: number; dlt: number }
export interface Snapshot { seq: number; totals: Totals }
export type StreamFrame =
  | { kind: 'telemetry'; seq: number; eventId: string; adapterId: string; status: Status; latencyMs: number; country: string; occurredAt: string }
  | { kind: 'duplicate'; seq: number; eventId: string; adapterId: string }
  | { kind: 'alert'; seq: number; alertId: string; adapterId: string; reason: string; raisedAt: string; triggerEventId: string }
  | { kind: 'dlt'; seq: number; reason: string }
  | { kind: 'heartbeat'; seq: number; totals: Totals }
export interface AdapterSummary { adapterId: string; consecutiveDown: number; alertActive: boolean; lastSeen: string }
export type ConnectionStatus = 'connecting' | 'live' | 'reconnecting'
export interface StreamHandlers {
  onFrame: (frame: StreamFrame) => void
  onStatus: (status: ConnectionStatus) => void
  onGap: () => void
}
export function connectStream(handlers: StreamHandlers, open: (url: string) => EventSourceLike = url => new EventSource(url)): () => void
export interface EventSourceLike {
  onopen: (() => void) | null
  onerror: (() => void) | null
  addEventListener(name: string, listener: (event: { data: string }) => void): void
  close(): void
}
```

- [ ] **Step 1: Write the failing test**:

```ts
import { expect, test, vi } from 'vitest'
import { connectStream, type EventSourceLike } from './sseClient'

class FakeEventSource implements EventSourceLike {
  url: string
  onopen: (() => void) | null = null
  onerror: (() => void) | null = null
  closed = false
  private listeners = new Map<string, (event: { data: string }) => void>()

  constructor(url: string) {
    this.url = url
  }

  addEventListener(name: string, listener: (event: { data: string }) => void) {
    this.listeners.set(name, listener)
  }

  emit(name: string, data: object) {
    this.listeners.get(name)?.({ data: JSON.stringify(data) })
  }

  close() {
    this.closed = true
  }
}

test('connects to the hub stream and stamps frames with their event kind', () => {
  const frames: unknown[] = []
  let source: FakeEventSource | undefined
  const close = connectStream(
    { onFrame: f => frames.push(f), onStatus: () => {}, onGap: () => {} },
    url => (source = new FakeEventSource(url)),
  )
  expect(source?.url).toBe('/api/v1/stream')
  source!.emit('telemetry', { seq: 1, eventId: 'e1', adapterId: 'gw-1', status: 'UP', latencyMs: 10, country: 'ES', occurredAt: '2026-09-15T10:00:00Z' })
  expect(frames[0]).toMatchObject({ kind: 'telemetry', seq: 1, adapterId: 'gw-1' })
  close()
  expect(source!.closed).toBe(true)
})

test('reports live and reconnecting status', () => {
  const statuses: string[] = []
  let source: FakeEventSource | undefined
  const close = connectStream(
    { onFrame: () => {}, onStatus: s => statuses.push(s), onGap: () => {} },
    url => (source = new FakeEventSource(url)),
  )
  source!.onopen!()
  source!.onerror!()
  expect(statuses).toEqual(['live', 'reconnecting'])
  close()
})

test('detects seq gaps and calls onGap once per gap', () => {
  const gaps: number[] = []
  let source: FakeEventSource | undefined
  const close = connectStream(
    { onFrame: () => {}, onStatus: () => {}, onGap: () => gaps.push(1) },
    url => (source = new FakeEventSource(url)),
  )
  source!.emit('telemetry', { seq: 1 })
  source!.emit('telemetry', { seq: 5 })
  source!.emit('telemetry', { seq: 6 })
  source!.emit('telemetry', { seq: 3 })
  expect(gaps).toHaveLength(1)
  close()
})
```

- [ ] **Step 2: Run to verify failure** — `npm --prefix dashboard test` → module `./sseClient` missing.
- [ ] **Step 3: Implement** `types.ts` (interfaces block above) and `sseClient.ts`:

```ts
import type { ConnectionStatus, StreamFrame } from './types'

export interface EventSourceLike {
  onopen: (() => void) | null
  onerror: (() => void) | null
  addEventListener(name: string, listener: (event: { data: string }) => void): void
  close(): void
}

export interface StreamHandlers {
  onFrame: (frame: StreamFrame) => void
  onStatus: (status: ConnectionStatus) => void
  onGap: () => void
}

const EVENT_NAMES = ['telemetry', 'duplicate', 'alert', 'dlt', 'heartbeat'] as const

export function connectStream(
  handlers: StreamHandlers,
  open: (url: string) => EventSourceLike = url => new EventSource(url),
): () => void {
  const source = open('/api/v1/stream')
  let lastSeq = 0
  source.onopen = () => handlers.onStatus('live')
  source.onerror = () => handlers.onStatus('reconnecting')
  for (const name of EVENT_NAMES) {
    source.addEventListener(name, event => {
      const parsed = JSON.parse(event.data) as { seq: number }
      if (lastSeq > 0 && parsed.seq > lastSeq + 1) handlers.onGap()
      if (parsed.seq > lastSeq) lastSeq = parsed.seq
      handlers.onFrame({ kind: name, ...parsed } as StreamFrame)
    })
  }
  return () => source.close()
}
```

- [ ] **Step 4: Run** — `npm --prefix dashboard test` → PASS.
- [ ] **Step 5: Commit**

```bash
git add dashboard
git commit -m "feat: stream types and sse client with seq-gap detection"
```

---

### Task 10: Series aggregation (pure)

**Files:**
- Create: `dashboard/src/stream/series.ts`, `dashboard/src/stream/series.test.ts`

**Interfaces:**
- Produces (consumed by store Task 11 and charts Task 14):

```ts
export const BUCKET_MS = 1000
export const MAX_BUCKETS = 120
export const LAT_BIN_MS = 100
export const LAT_BINS = 60
export const SAMPLE_CAP = 64
export interface Bucket { second: number; count: number; samples: number[] }
export interface Series { buckets: Bucket[]; resets: number[] }
export function emptySeries(): Series
export function foldTelemetry(series: Series, latencyMs: number, nowMs: number): Series
export function markReset(series: Series, nowMs: number): Series
export function rateAt(series: Series, second: number): number
export function latenciesWithin(series: Series, fromSecond: number): number[]
export function histogram(series: Series, fromSecond: number): number[]
export function percentile(samples: number[], q: number): number
```

- [ ] **Step 1: Write the failing test**:

```ts
import { describe, expect, test } from 'vitest'
import { emptySeries, foldTelemetry, histogram, latenciesWithin, markReset, percentile, rateAt } from './series'

describe('foldTelemetry', () => {
  test('counts events into one-second buckets', () => {
    let s = emptySeries()
    s = foldTelemetry(s, 100, 1000)
    s = foldTelemetry(s, 200, 1500)
    s = foldTelemetry(s, 300, 2500)
    expect(rateAt(s, 1)).toBe(2)
    expect(rateAt(s, 2)).toBe(1)
  })

  test('keeps at most 120 buckets', () => {
    let s = emptySeries()
    for (let i = 0; i < 150; i++) {
      s = foldTelemetry(s, 10, i * 1000)
    }
    expect(s.buckets).toHaveLength(120)
    expect(s.buckets[0].second).toBe(30)
  })
})

describe('percentile', () => {
  test('returns 0 on empty samples', () => {
    expect(percentile([], 0.95)).toBe(0)
  })

  test('returns the nearest-rank value', () => {
    const samples = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    expect(percentile(samples, 0.5)).toBe(50)
    expect(percentile(samples, 0.95)).toBe(100)
  })
})

describe('histogram', () => {
  test('bins latencies in 100ms bins, clamping the last bin', () => {
    let s = emptySeries()
    s = foldTelemetry(s, 50, 1000)
    s = foldTelemetry(s, 150, 1000)
    s = foldTelemetry(s, 9999, 1000)
    const bins = histogram(s, 0)
    expect(bins[0]).toBe(1)
    expect(bins[1]).toBe(1)
    expect(bins[59]).toBe(1)
    expect(bins).toHaveLength(60)
  })
})

describe('latenciesWithin', () => {
  test('collects samples from the given second on', () => {
    let s = emptySeries()
    s = foldTelemetry(s, 10, 0)
    s = foldTelemetry(s, 20, 1000)
    expect(latenciesWithin(s, 1)).toEqual([20])
  })
})

test('markReset records the second of the gap', () => {
  const s = markReset(foldTelemetry(emptySeries(), 10, 5000), 9000)
  expect(s.resets).toEqual([9])
})
```

- [ ] **Step 2: Run to verify failure** — `npm --prefix dashboard test` → module missing.
- [ ] **Step 3: Implement** `series.ts`:

```ts
export const BUCKET_MS = 1000
export const MAX_BUCKETS = 120
export const LAT_BIN_MS = 100
export const LAT_BINS = 60
export const SAMPLE_CAP = 64

export interface Bucket {
  second: number
  count: number
  samples: number[]
}

export interface Series {
  buckets: Bucket[]
  resets: number[]
}

export function emptySeries(): Series {
  return { buckets: [], resets: [] }
}

export function foldTelemetry(series: Series, latencyMs: number, nowMs: number): Series {
  const second = Math.floor(nowMs / BUCKET_MS)
  const last = series.buckets.at(-1)
  const previous = last && last.second === second ? last : undefined
  const samples = previous && previous.samples.length < SAMPLE_CAP ? [...previous.samples, latencyMs] : previous ? previous.samples : [latencyMs]
  const bucket: Bucket = { second, count: (previous?.count ?? 0) + 1, samples }
  const buckets = previous ? [...series.buckets.slice(0, -1), bucket] : [...series.buckets, bucket]
  return { resets: series.resets, buckets: buckets.slice(-MAX_BUCKETS) }
}

export function markReset(series: Series, nowMs: number): Series {
  return { buckets: series.buckets, resets: [...series.resets, Math.floor(nowMs / BUCKET_MS)] }
}

export function rateAt(series: Series, second: number): number {
  return series.buckets.find(bucket => bucket.second === second)?.count ?? 0
}

export function latenciesWithin(series: Series, fromSecond: number): number[] {
  return series.buckets.filter(bucket => bucket.second >= fromSecond).flatMap(bucket => bucket.samples)
}

export function histogram(series: Series, fromSecond: number): number[] {
  const bins = Array.from({ length: LAT_BINS }, () => 0)
  for (const latency of latenciesWithin(series, fromSecond)) {
    const bin = Math.min(Math.floor(latency / LAT_BIN_MS), LAT_BINS - 1)
    bins[bin] += 1
  }
  return bins
}

export function percentile(samples: number[], q: number): number {
  if (samples.length === 0) return 0
  const sorted = [...samples].sort((a, b) => a - b)
  return sorted[Math.max(0, Math.ceil(q * sorted.length) - 1)]
}
```

- [ ] **Step 4: Run** — `npm --prefix dashboard test` → PASS.
- [ ] **Step 5: Commit**

```bash
git add dashboard
git commit -m "feat: client-side series aggregation (throughput buckets, latency histogram, percentiles)"
```

---

### Task 11: `DashboardStore` — single store, rAF-coalesced

**Files:**
- Create: `dashboard/src/stream/store.ts`, `dashboard/src/stream/store.test.ts`

**Interfaces:**
- Consumes: `StreamFrame`, `Snapshot`, `AdapterSummary` (Task 9), series functions (Task 10).
- Produces (used by every component and the lifecycle):

```ts
export interface AlertItem { seq: number; alertId: string; adapterId: string; reason: string; raisedAt: string }
export interface DltItem { seq: number; reason: string }
export interface PhaseState { name: string; index: number; total: number }
export interface DashboardState {
  status: ConnectionStatus
  seq: number
  totals: Totals          // hub truth: snapshot + heartbeat only
  sessionEvents: number   // telemetry frames received this session
  alertsFeed: AlertItem[] // cap 50, newest first
  dltFeed: DltItem[]      // cap 20, newest first
  series: Series
  adapters: AdapterSummary[]
  phase: PhaseState | null
}
export const store: DashboardStore
export function useDashboard<T>(selector: (state: DashboardState) => T): T  // selector must return primitives or stable references
// store methods: apply(frame), applySnapshot(s), markReset(), setAdapters(a), setStatus(s), setPhase(p), flush()
```

- [ ] **Step 1: Write the failing test** (jsdom provides `requestAnimationFrame`; `flush()` makes notifications deterministic):

```ts
import { beforeEach, describe, expect, test, vi } from 'vitest'
import { store } from './store'

const telemetry = (seq: number, latencyMs = 100) =>
  ({ kind: 'telemetry', seq, eventId: `e${seq}`, adapterId: 'gw-1', status: 'UP', latencyMs, country: 'ES', occurredAt: '2026-09-15T10:00:00Z' })

describe('store.apply', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-09-15T10:00:30Z'))
  })

  test('counts session events and folds telemetry into the series', () => {
    store.apply(telemetry(1))
    store.apply(telemetry(2))
    store.flush()
    const state = store.getState()
    expect(state.sessionEvents).toBe(2)
    expect(state.series.buckets.at(-1)?.count).toBe(2)
    expect(state.seq).toBe(2)
  })

  test('ignores out-of-order frames and marks a reset on gaps', () => {
    store.apply(telemetry(1))
    store.apply(telemetry(5))
    store.flush()
    const state = store.getState()
    expect(state.seq).toBe(5)
    expect(state.series.resets).toHaveLength(1)
  })

  test('alerts and dlt frames feed their lists with caps', () => {
    for (let i = 1; i <= 55; i++) {
      store.apply({ kind: 'alert', seq: i, alertId: `a${i}`, adapterId: 'gw-1', reason: '3 DOWN', raisedAt: '2026-09-15T10:00:00Z', triggerEventId: `e${i}` })
    }
    store.apply({ kind: 'dlt', seq: 100, reason: 'broken' })
    store.flush()
    expect(store.getState().alertsFeed).toHaveLength(50)
    expect(store.getState().alertsFeed[0].alertId).toBe('a55')
    expect(store.getState().dltFeed[0].reason).toBe('broken')
  })

  test('snapshot and heartbeat refresh seq and totals', () => {
    store.applySnapshot({ seq: 9, totals: { duplicates: 3, alerts: 1, dlt: 2 } })
    store.apply({ kind: 'heartbeat', seq: 11, totals: { duplicates: 4, alerts: 1, dlt: 3 } })
    store.flush()
    const state = store.getState()
    expect(state.seq).toBe(11)
    expect(state.totals).toEqual({ duplicates: 4, alerts: 1, dlt: 3 })
  })
})
```

- [ ] **Step 2: Run to verify failure** — module missing.
- [ ] **Step 3: Implement** `store.ts`:

```ts
import { useSyncExternalStore } from 'react'
import { emptySeries, foldTelemetry, markReset, type Series } from './series'
import type { AdapterSummary, ConnectionStatus, Snapshot, StreamFrame, Totals } from './types'

export interface AlertItem {
  seq: number
  alertId: string
  adapterId: string
  reason: string
  raisedAt: string
}

export interface DltItem {
  seq: number
  reason: string
}

export interface PhaseState {
  name: string
  index: number
  total: number
}

export interface DashboardState {
  status: ConnectionStatus
  seq: number
  totals: Totals
  sessionEvents: number
  alertsFeed: AlertItem[]
  dltFeed: DltItem[]
  series: Series
  adapters: AdapterSummary[]
  phase: PhaseState | null
}

const ALERT_CAP = 50
const DLT_CAP = 20

const initialState: DashboardState = {
  status: 'connecting',
  seq: 0,
  totals: { duplicates: 0, alerts: 0, dlt: 0 },
  sessionEvents: 0,
  alertsFeed: [],
  dltFeed: [],
  series: emptySeries(),
  adapters: [],
  phase: null,
}

export class DashboardStore {
  private state: DashboardState = initialState
  private listeners = new Set<() => void>()
  private pending: number | null = null

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener)
    return () => this.listeners.delete(listener)
  }

  getState = (): DashboardState => this.state

  apply(frame: StreamFrame): void {
    if (frame.kind === 'heartbeat') {
      this.set({ seq: frame.seq, totals: frame.totals })
      return
    }
    if (frame.seq <= this.state.seq) return
    const patch: Partial<DashboardState> = { seq: frame.seq }
    switch (frame.kind) {
      case 'telemetry':
        patch.sessionEvents = this.state.sessionEvents + 1
        patch.series = foldTelemetry(this.state.series, frame.latencyMs, Date.now())
        if (frame.seq > this.state.seq + 1) patch.series = markReset(patch.series, Date.now())
        break
      case 'alert': {
        const item: AlertItem = { seq: frame.seq, alertId: frame.alertId, adapterId: frame.adapterId, reason: frame.reason, raisedAt: frame.raisedAt }
        patch.alertsFeed = [item, ...this.state.alertsFeed].slice(0, ALERT_CAP)
        break
      }
      case 'dlt': {
        const item: DltItem = { seq: frame.seq, reason: frame.reason }
        patch.dltFeed = [item, ...this.state.dltFeed].slice(0, DLT_CAP)
        break
      }
      case 'duplicate':
        break
    }
    this.set(patch)
  }

  applySnapshot(snapshot: Snapshot): void {
    this.set({ seq: snapshot.seq, totals: snapshot.totals })
  }

  markReset(): void {
    this.set({ series: markReset(this.state.series, Date.now()) })
  }

  setAdapters(adapters: AdapterSummary[]): void {
    this.set({ adapters })
  }

  setStatus(status: ConnectionStatus): void {
    this.set({ status })
  }

  setPhase(phase: PhaseState | null): void {
    this.set({ phase })
  }

  flush(): void {
    if (this.pending !== null) {
      cancelAnimationFrame(this.pending)
      this.pending = null
      this.listeners.forEach(listener => listener())
    }
  }

  private set(patch: Partial<DashboardState>): void {
    this.state = { ...this.state, ...patch }
    if (this.pending !== null) return
    this.pending = requestAnimationFrame(() => {
      this.pending = null
      this.listeners.forEach(listener => listener())
    })
  }
}

export const store = new DashboardStore()

export function useDashboard<T>(selector: (state: DashboardState) => T): T {
  return useSyncExternalStore(store.subscribe, () => selector(store.getState()))
}
```

- [ ] **Step 4: Run** — `npm --prefix dashboard test` → PASS (fake timers + `flush()` keep it deterministic).
- [ ] **Step 5: Commit**

```bash
git add dashboard
git commit -m "feat: dashboard store with rAF-coalesced notifications and caps"
```

---

### Task 12: API clients

**Files:**
- Create: `dashboard/src/api/hub.ts`, `dashboard/src/api/gateway.ts`, `dashboard/src/api/api.test.ts`

**Interfaces:**
- Consumes: hub endpoints (`/api/v1/metrics/snapshot`, `/api/v1/adapters`), gateway `POST /api/v1/telemetry/simulate?profile=…` (CORS from Task 6).
- Produces: `fetchSnapshot()`, `fetchAdapters()`, `simulate(profile)` — `VITE_GATEWAY_URL` overrides the gateway base.

```ts
// dashboard/src/api/hub.ts
import type { AdapterSummary, Snapshot } from '../stream/types'

async function getJson<T>(url: string): Promise<T> {
  const response = await fetch(url)
  if (!response.ok) throw new Error(`GET ${url} -> ${response.status}`)
  return response.json() as Promise<T>
}

export const fetchSnapshot = (): Promise<Snapshot> => getJson<Snapshot>('/api/v1/metrics/snapshot')
export const fetchAdapters = (): Promise<AdapterSummary[]> => getJson<AdapterSummary[]>('/api/v1/adapters')
```

```ts
// dashboard/src/api/gateway.ts
const GATEWAY = import.meta.env.VITE_GATEWAY_URL ?? 'http://localhost:8081'

export async function simulate(profile: string): Promise<void> {
  const response = await fetch(`${GATEWAY}/api/v1/telemetry/simulate?profile=${profile}`, { method: 'POST' })
  if (!response.ok) throw new Error(`simulate(${profile}) -> ${response.status}`)
}
```

- [ ] **Step 1: Write the failing test**:

```ts
import { afterEach, describe, expect, test, vi } from 'vitest'
import { fetchAdapters, fetchSnapshot } from './hub'
import { simulate } from './gateway'

afterEach(() => vi.unstubAllGlobals())

describe('hub client', () => {
  test('fetchSnapshot parses the snapshot payload', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response(JSON.stringify({ seq: 42, totals: { duplicates: 1, alerts: 2, dlt: 3 } }), { status: 200 })))
    const snapshot = await fetchSnapshot()
    expect(snapshot.seq).toBe(42)
    expect(snapshot.totals.dlt).toBe(3)
  })

  test('fetchSnapshot throws on non-2xx', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response('nope', { status: 503 })))
    await expect(fetchSnapshot()).rejects.toThrow('503')
  })

  test('fetchAdapters returns the list', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response(JSON.stringify([{ adapterId: 'gw-1', consecutiveDown: 0, alertActive: false, lastSeen: '2026-09-15T10:00:00Z' }]), { status: 200 })))
    const adapters = await fetchAdapters()
    expect(adapters[0].adapterId).toBe('gw-1')
  })
})

describe('gateway client', () => {
  test('simulate POSTs the profile to the gateway', async () => {
    const fetchMock = vi.fn(async () => new Response('{"accepted":20}', { status: 200 }))
    vi.stubGlobal('fetch', fetchMock)
    await simulate('low')
    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringMatching(/\/api\/v1\/telemetry\/simulate\?profile=low$/),
      { method: 'POST' },
    )
  })

  test('simulate throws on failure', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response('bad profile', { status: 400 })))
    await expect(simulate('nope')).rejects.toThrow('400')
  })
})
```

- [ ] **Step 2: Run to verify failure** — modules missing.
- [ ] **Step 3: Implement** both files (code above). Add `dashboard/src/vite-env.d.ts` line `/// <reference types="vite/client" />` (template includes it).
- [ ] **Step 4: Run** — `npm --prefix dashboard test` → PASS.
- [ ] **Step 5: Commit**

```bash
git add dashboard
git commit -m "feat: hub and gateway api clients"
```

---

### Task 13: Demo control state machine (`runFullDemo`)

**Files:**
- Create: `dashboard/src/demo/runFullDemo.ts`, `dashboard/src/demo/runFullDemo.test.ts`

**Interfaces:**
- Consumes: `simulate` (Task 12), `fetchSnapshot` (Task 12).
- Produces (consumed by `DemoControl` Task 15):

```ts
export interface DemoDeps {
  simulate: (profile: string) => Promise<void>
  snapshot: () => Promise<{ seq: number }>
  onPhase?: (name: string, index: number, total: number) => void
  shouldStop?: () => boolean
  sleep?: (ms: number) => Promise<void>
}
export const PHASES: readonly string[] // ['low', 'high', 'overload', 'overload'] — mirrors demo.sh
export async function runFullDemo(deps: DemoDeps): Promise<'completed' | 'aborted'>
```

Stability rule (mirrors `demo.sh wait_events_stable`): after each phase, poll the snapshot every 2 s until `seq` is unchanged 3 consecutive times.

- [ ] **Step 1: Write the failing test**:

```ts
import { describe, expect, test } from 'vitest'
import { runFullDemo } from './runFullDemo'

const sleep = () => Promise.resolve()
const stableSnapshot = (seq: number) => async () => ({ seq })

describe('runFullDemo', () => {
  test('runs low, high, overload, overload in order and completes', async () => {
    const ran: string[] = []
    const phases: string[] = []
    let seq = 0
    const result = await runFullDemo({
      simulate: async profile => {
        ran.push(profile)
        seq += 10
      },
      snapshot: async () => ({ seq }),
      onPhase: name => phases.push(name),
      sleep,
    })
    expect(ran).toEqual(['low', 'high', 'overload', 'overload'])
    expect(phases).toEqual(['low', 'high', 'overload', 'overload'])
    expect(result).toBe('completed')
  })

  test('aborts between phases when shouldStop flips', async () => {
    const ran: string[] = []
    let stopped = false
    const result = await runFullDemo({
      simulate: async profile => ran.push(profile),
      snapshot: stableSnapshot(1),
      shouldStop: () => stopped,
      sleep: () => {
        stopped = true
        return Promise.resolve()
      },
    })
    expect(result).toBe('aborted')
    expect(ran).toEqual(['low'])
  })

  test('waits for a stable seq before the next phase', async () => {
    const snapshots: number[] = []
    let seq = 0
    await runFullDemo({
      simulate: async () => {
        seq += 5
      },
      snapshot: async () => {
        snapshots.push(seq)
        return { seq }
      },
      sleep,
    })
    // last three reads before each next phase saw the same seq
    expect(snapshots.slice(-3)).toEqual([seq, seq, seq])
  })
})
```

- [ ] **Step 2: Run to verify failure** — module missing.
- [ ] **Step 3: Implement**:

```ts
export interface DemoDeps {
  simulate: (profile: string) => Promise<void>
  snapshot: () => Promise<{ seq: number }>
  onPhase?: (name: string, index: number, total: number) => void
  shouldStop?: () => boolean
  sleep?: (ms: number) => Promise<void>
}

export const PHASES: readonly string[] = ['low', 'high', 'overload', 'overload']
const STABLE_READS = 3
const POLL_MS = 2000

export async function runFullDemo(deps: DemoDeps): Promise<'completed' | 'aborted'> {
  const sleep = deps.sleep ?? (ms => new Promise(resolve => setTimeout(resolve, ms)))
  for (const [index, name] of PHASES.entries()) {
    if (deps.shouldStop?.()) return 'aborted'
    deps.onPhase?.(name, index + 1, PHASES.length)
    await deps.simulate(name)
    await waitForStable(deps, sleep)
  }
  return 'completed'
}

async function waitForStable(deps: DemoDeps, sleep: (ms: number) => Promise<void>): Promise<void> {
  let previous = -1
  let stable = 0
  while (stable < STABLE_READS) {
    await sleep(POLL_MS)
    if (deps.shouldStop?.()) return
    const current = (await deps.snapshot()).seq
    if (current === previous) {
      stable += 1
    } else {
      stable = 0
      previous = current
    }
  }
}
```

- [ ] **Step 4: Run** — `npm --prefix dashboard test` → PASS.
- [ ] **Step 5: Commit**

```bash
git add dashboard
git commit -m "feat: run-full-demo state machine with seq-stability wait"
```

---

### Task 14: Charts — option builders + ECharts components

**Files:**
- Create: `dashboard/src/charts/options.ts`, `dashboard/src/charts/options.test.ts`
- Create: `dashboard/src/components/ThroughputChart.tsx`, `dashboard/src/components/LatencyChart.tsx`

**Interfaces:**
- Consumes: `Series`, `histogram`, `latenciesWithin`, `percentile` (Task 10); `store`, `useDashboard` (Task 11).
- Produces: `throughputOption(series)`, `latencyOption(series, windowSeconds)` (pure, tested) and two live components rendering into `Panel`s with the SVG renderer.

- [ ] **Step 1: Write the failing test**:

```ts
import { describe, expect, test } from 'vitest'
import { emptySeries, foldTelemetry } from '../stream/series'
import { latencyOption, throughputOption } from './options'

describe('throughputOption', () => {
  test('fills missing seconds with zero', () => {
    let series = emptySeries()
    series = foldTelemetry(series, 10, 0)
    series = foldTelemetry(series, 10, 2000)
    const option = throughputOption(series)
    const data = option.series[0].data as number[]
    expect(data).toEqual([1, 0, 1])
  })

  test('adds a reset markLine per recorded reset', () => {
    let series = emptySeries()
    series = foldTelemetry(series, 10, 0)
    series = markReset(series, 5000)
    const option = throughputOption(series)
    const marks = option.series[0].markLine.data as unknown[]
    expect(marks).toHaveLength(1)
  })
})

describe('latencyOption', () => {
  test('returns 60 bins and marks p50/p95', () => {
    let series = emptySeries()
    for (let i = 0; i < 10; i++) {
      series = foldTelemetry(series, i * 10, i * 1000)
    }
    const option = latencyOption(series, 60)
    expect(option.series[0].data).toHaveLength(60)
    const marks = option.series[0].markLine.data as { yAxis: number }[]
    expect(marks).toHaveLength(2)
    expect(marks[0].yAxis).toBeGreaterThan(0)
  })
})
```

Import `markReset` from `../stream/series` too.

- [ ] **Step 2: Run to verify failure** — module missing.
- [ ] **Step 3: Implement** `options.ts`:

```ts
import type { EChartsOption } from 'echarts'
import { histogram, latenciesWithin, percentile, type Series } from '../stream/series'

const clock = (second: number) => new Date(second * 1000).toLocaleTimeString('en-GB', { hour12: false })
const AXIS = { color: '#4d5a6d', fontSize: 9 } as const
const GRID = { top: 8, right: 8, bottom: 20, left: 36 } as const

export function throughputOption(series: Series): EChartsOption {
  const first = series.buckets[0]?.second ?? 0
  const last = series.buckets.at(-1)?.second ?? 0
  const categories: string[] = []
  const counts: number[] = []
  for (let second = first; second <= last; second++) {
    categories.push(clock(second))
    counts.push(series.buckets.find(bucket => bucket.second === second)?.count ?? 0)
  }
  return {
    animation: false,
    backgroundColor: 'transparent',
    grid: GRID,
    xAxis: { type: 'category', data: categories, axisLabel: AXIS, axisLine: { lineStyle: { color: '#1c2530' } } },
    yAxis: { type: 'value', axisLabel: AXIS, splitLine: { lineStyle: { color: '#121826' } } },
    series: [{
      type: 'bar',
      data: counts,
      itemStyle: { color: '#3ddc97' },
      barWidth: '70%',
      markLine: {
        symbol: 'none',
        label: { show: false },
        lineStyle: { color: '#ff5470' },
        data: series.resets.map(second => ({ xAxis: clock(second) })),
      },
    }],
  }
}

export function latencyOption(series: Series, windowSeconds: number): EChartsOption {
  const fromSecond = Math.floor(Date.now() / 1000) - windowSeconds
  const samples = latenciesWithin(series, fromSecond)
  const bins = histogram(series, fromSecond)
  return {
    animation: false,
    backgroundColor: 'transparent',
    grid: GRID,
    xAxis: {
      type: 'category',
      data: bins.map((_, index) => `${index * 100}`),
      axisLabel: { ...AXIS, interval: 9 },
      axisLine: { lineStyle: { color: '#1c2530' } },
      name: 'ms',
      nameTextStyle: AXIS,
    },
    yAxis: { type: 'value', axisLabel: AXIS, splitLine: { lineStyle: { color: '#121826' } } },
    series: [{
      type: 'bar',
      data: bins,
      itemStyle: { color: '#ffb347' },
      markLine: {
        symbol: 'none',
        label: { formatter: '{b}: {c}', color: '#8b98ab', fontSize: 9 },
        lineStyle: { color: '#3ddc97', type: 'dashed' },
        data: [
          { yAxis: percentile(samples, 0.5), name: 'p50' },
          { yAxis: percentile(samples, 0.95), name: 'p95' },
        ],
      },
    }],
  }
}
```

Components (same shape, different option/label; `ThroughputChart.tsx` shown, `LatencyChart.tsx` mirrors it with `latencyOption(store.getState().series, 60)` and label `latency · ms`):

```tsx
import { useEffect, useRef } from 'react'
import * as echarts from 'echarts'
import { store, useDashboard } from '../stream/store'
import { throughputOption } from '../charts/options'
import { Panel } from './Panel'

export function ThroughputChart() {
  const holder = useRef<HTMLDivElement>(null)
  const chart = useRef<echarts.ECharts | null>(null)
  const series = useDashboard(state => state.series)

  useEffect(() => {
    if (!holder.current) return
    chart.current = echarts.init(holder.current, null, { renderer: 'svg' })
    const onResize = () => chart.current?.resize()
    window.addEventListener('resize', onResize)
    return () => {
      window.removeEventListener('resize', onResize)
      chart.current?.dispose()
      chart.current = null
    }
  }, [])

  useEffect(() => {
    chart.current?.setOption(throughputOption(series))
  }, [series])

  return (
    <Panel label="throughput · ev/s">
      <div ref={holder} className="h-full min-h-24 w-full" />
    </Panel>
  )
}
```

- [ ] **Step 4: Run** — `npm --prefix dashboard test` → PASS; wire both components into `App.tsx` replacing the placeholders; `npm --prefix dashboard run build` → OK.
- [ ] **Step 5: Commit**

```bash
git add dashboard
git commit -m "feat: throughput and latency charts fed by the dashboard store"
```

---

### Task 15: Panels and app wiring — the full Mission Control

**Files:**
- Create: `dashboard/src/components/Ticker.tsx`, `PipelineFlow.tsx`, `AdapterWall.tsx`, `AlertsFeed.tsx`, `IntegrityPanel.tsx`, `DemoControl.tsx`, `ChapterSlate.tsx`, each with a `*.test.tsx`
- Create: `dashboard/src/app/lifecycle.ts`, `dashboard/src/app/lifecycle.test.ts`
- Modify: `dashboard/src/App.tsx`

**Interfaces:**
- Consumes: everything from Tasks 9–14.
- Produces: `startDashboard(): () => void` — connects the SSE stream, loads snapshot + adapters, polls adapters every 3 s, on gap marks reset and refetches the snapshot. Components read the store only via `useDashboard` selectors.

- [ ] **Step 1: Write the failing tests** (one focused test per component; the essential ones):

`lifecycle.test.ts`:

```ts
import { expect, test, vi } from 'vitest'
import { store } from '../stream/store'
import { startDashboard } from './lifecycle'

vi.mock('../stream/sseClient', () => ({
  connectStream: (handlers: { onFrame: (f: unknown) => void }) => {
    handlers.onFrame({ kind: 'telemetry', seq: 1, eventId: 'e', adapterId: 'gw-1', status: 'UP', latencyMs: 10, country: 'ES', occurredAt: 'x' })
    return () => {}
  },
}))

vi.mock('../api/hub', () => ({
  fetchSnapshot: async () => ({ seq: 0, totals: { duplicates: 7, alerts: 0, dlt: 0 } }),
  fetchAdapters: async () => [{ adapterId: 'gw-1', consecutiveDown: 0, alertActive: false, lastSeen: 'x' }],
}))

test('startDashboard loads snapshot and adapters and streams frames', async () => {
  const stop = startDashboard()
  await vi.waitFor(() => {
    expect(store.getState().totals.duplicates).toBe(7)
    expect(store.getState().adapters[0].adapterId).toBe('gw-1')
    expect(store.getState().sessionEvents).toBe(1)
  })
  stop()
})
```

`DemoControl.test.tsx`:

```tsx
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, test, vi } from 'vitest'
import { DemoControl } from './DemoControl'

const simulate = vi.fn(async () => {})

vi.mock('../api/gateway', () => ({ simulate: (profile: string) => simulate(profile) }))

test('profile buttons fire the gateway simulate call', async () => {
  render(<DemoControl />)
  await userEvent.click(screen.getByRole('button', { name: 'overload' }))
  expect(simulate).toHaveBeenCalledWith('overload')
})
```

`Ticker.test.tsx`:

```tsx
import { render, screen } from '@testing-library/react'
import { expect, test } from 'vitest'
import { store } from '../stream/store'
import { Ticker } from './Ticker'

test('ticker shows hub totals and the live session count', () => {
  store.applySnapshot({ seq: 9, totals: { duplicates: 95, alerts: 14, dlt: 20 } })
  store.flush()
  render(<Ticker />)
  expect(screen.getByText('95')).toBeInTheDocument()
  expect(screen.getByText('14')).toBeInTheDocument()
  expect(screen.getByText('20')).toBeInTheDocument()
})
```

`AlertsFeed.test.tsx` (feed cap already covered by store tests — here assert rendering):

```tsx
import { render, screen } from '@testing-library/react'
import { expect, test } from 'vitest'
import { store } from '../stream/store'
import { AlertsFeed } from './AlertsFeed'

test('renders the newest alert first', () => {
  store.apply({ kind: 'alert', seq: 1, alertId: 'a1', adapterId: 'gw-1', reason: '3 DOWN', raisedAt: 'x', triggerEventId: 'e1' })
  store.apply({ kind: 'alert', seq: 2, alertId: 'a2', adapterId: 'gw-2', reason: '3 DOWN', raisedAt: 'x', triggerEventId: 'e2' })
  store.flush()
  render(<AlertsFeed />)
  const items = screen.getAllByText(/gw-/)
  expect(items[0]).toHaveTextContent('gw-2')
})
```

`AdapterWall.test.tsx`:

```tsx
import { render, screen } from '@testing-library/react'
import { expect, test } from 'vitest'
import { store } from '../stream/store'
import { AdapterWall } from './AdapterWall'

test('wall lists every adapter from the read api', () => {
  store.setAdapters([
    { adapterId: 'gw-es-1', consecutiveDown: 0, alertActive: false, lastSeen: 'x' },
    { adapterId: 'gw-de-2', consecutiveDown: 3, alertActive: true, lastSeen: 'x' },
  ])
  store.flush()
  render(<AdapterWall />)
  expect(screen.getByText('gw-es-1')).toBeInTheDocument()
  expect(screen.getByText('gw-de-2')).toBeInTheDocument()
  expect(screen.getByText(/alert active/)).toBeInTheDocument()
})
```

- [ ] **Step 2: Run to verify failure** — components/modules missing.
- [ ] **Step 3: Implement.** `lifecycle.ts`:

```ts
import { fetchAdapters, fetchSnapshot } from '../api/hub'
import { connectStream } from '../stream/sseClient'
import { store } from '../stream/store'

export function startDashboard(): () => void {
  store.setStatus('connecting')
  void refreshSnapshot()
  void refreshAdapters()
  const poll = window.setInterval(() => void refreshAdapters(), 3000)
  const close = connectStream({
    onFrame: frame => store.apply(frame),
    onStatus: status => store.setStatus(status),
    onGap: () => {
      store.markReset()
      void refreshSnapshot()
    },
  })
  return () => {
    window.clearInterval(poll)
    close()
  }
}

async function refreshSnapshot(): Promise<void> {
  try {
    store.applySnapshot(await fetchSnapshot())
  } catch {
    store.setStatus('reconnecting')
  }
}

async function refreshAdapters(): Promise<void> {
  try {
    store.setAdapters(await fetchAdapters())
  } catch {
    // keep the last known wall; the next poll retries
  }
}
```

`Ticker.tsx`:

```tsx
import { useDashboard } from '../stream/store'

const format = (n: number) => n.toLocaleString('en-US')

export function Ticker() {
  const totals = useDashboard(state => state.totals)
  const session = useDashboard(state => state.sessionEvents)
  const phase = useDashboard(state => state.phase)
  const status = useDashboard(state => state.status)
  return (
    <div className="flex items-center gap-6 rounded-lg border border-line bg-panel px-4 py-2 text-xs">
      <span className={status === 'live' ? 'text-up' : 'text-warn'}>
        ● {status.toUpperCase()}
      </span>
      <span><span className="text-dim">events</span> {format(session)}</span>
      <span><span className="text-dim">duplicates</span> {format(totals.duplicates)}</span>
      <span><span className="text-dim">alerts</span> {format(totals.alerts)}</span>
      <span><span className="text-dim">dlt</span> {format(totals.dlt)}</span>
      {phase && (
        <span className="ml-auto text-up">
          PHASE {phase.index}/{phase.total} — {phase.name.toUpperCase()}
        </span>
      )}
    </div>
  )
}
```

`PipelineFlow.tsx`:

```tsx
import { useDashboard } from '../stream/store'
import { Panel } from './Panel'

const FLOW: { label: string; hint: string }[] = [
  { label: 'ADAPTERS', hint: 'POST /telemetry' },
  { label: 'KAFKA', hint: 'adapter.telemetry.v1' },
  { label: 'HUB', hint: 'idempotent upsert' },
  { label: 'ORACLE', hint: 'TELEMETRY_EVENT' },
]

export function PipelineFlow() {
  const session = useDashboard(state => state.sessionEvents)
  const seq = useDashboard(state => state.seq)
  const alerts = useDashboard(state => state.totals.alerts)
  return (
    <Panel label="pipeline">
      <div className="flex h-full flex-col justify-center gap-3 text-[10px]">
        {FLOW.map((node, index) => (
          <div key={node.label} className="flex items-center gap-3">
            <span className="w-20 shrink-0 text-dim">{node.label}</span>
            <span className="w-28 shrink-0 text-muted">{node.hint}</span>
            <svg viewBox="0 0 120 8" className="h-2 flex-1" preserveAspectRatio="none" aria-hidden>
              <line x1="0" y1="4" x2="120" y2="4" stroke="#1c2530" strokeWidth="2" />
              <line x1="0" y1="4" x2="120" y2="4" stroke="#3ddc97" strokeWidth="2" strokeDasharray="6 14" className="flow-line" />
            </svg>
            <span className="w-20 shrink-0 text-right text-fg">
              {index === 0 ? `${session} ev` : index === 2 ? `seq ${seq}` : index === 3 ? `${alerts} alerts` : ''}
            </span>
          </div>
        ))}
      </div>
    </Panel>
  )
}
```

`AdapterWall.tsx`:

```tsx
import { useDashboard } from '../stream/store'
import { Panel } from './Panel'

export function AdapterWall() {
  const adapters = useDashboard(state => state.adapters)
  return (
    <Panel label={`adapters · ${adapters.length}`}>
      <div className="grid grid-cols-3 gap-2 md:grid-cols-6">
        {adapters.map(adapter => {
          const down = adapter.consecutiveDown >= 3 || adapter.alertActive
          const degraded = !down && adapter.consecutiveDown > 0
          return (
            <div key={adapter.adapterId} className={`rounded border px-2 py-1.5 text-[10px] ${down ? 'border-down' : degraded ? 'border-warn' : 'border-line'}`}>
              <div className="truncate text-fg">{adapter.adapterId}</div>
              <div className={down ? 'text-down' : degraded ? 'text-warn' : 'text-up'}>
                {down ? 'DOWN' : degraded ? 'DEGRADED' : 'UP'} · {adapter.consecutiveDown}↓
              </div>
              {adapter.alertActive && <div className="text-down">alert active</div>}
            </div>
          )
        })}
      </div>
    </Panel>
  )
}
```

`AlertsFeed.tsx`:

```tsx
import { AnimatePresence, motion } from 'framer-motion'
import { useDashboard } from '../stream/store'
import { Panel } from './Panel'

export function AlertsFeed() {
  const feed = useDashboard(state => state.alertsFeed)
  return (
    <Panel label={`alerts · ${feed.length}`}>
      <ul className="flex flex-col gap-1 overflow-hidden text-[10px]">
        <AnimatePresence initial={false}>
          {feed.slice(0, 12).map(item => (
            <motion.li
              key={item.seq}
              layout
              initial={{ opacity: 0, x: 12 }}
              animate={{ opacity: 1, x: 0 }}
              exit={{ opacity: 0 }}
              className="rounded border border-down/40 bg-panel px-2 py-1"
            >
              <span className="text-down">▲ {item.adapterId}</span>{' '}
              <span className="text-muted">{item.reason}</span>
            </motion.li>
          ))}
        </AnimatePresence>
      </ul>
    </Panel>
  )
}
```

`IntegrityPanel.tsx`:

```tsx
import { useDashboard } from '../stream/store'
import { Panel } from './Panel'

export function IntegrityPanel() {
  const totals = useDashboard(state => state.totals)
  const dltFeed = useDashboard(state => state.dltFeed)
  return (
    <Panel label="integrity">
      <div className="flex gap-6">
        <div>
          <div className="num">{totals.duplicates}</div>
          <div className="cap">duplicates rejected</div>
        </div>
        <div>
          <div className="num">{totals.dlt}</div>
          <div className="cap">dead letters</div>
        </div>
      </div>
      <ul className="mt-2 space-y-0.5 text-[9px] text-muted">
        {dltFeed.slice(0, 3).map(item => (
          <li key={item.seq} className="truncate">dlt · {item.reason}</li>
        ))}
      </ul>
    </Panel>
  )
}
```

`DemoControl.tsx`:

```tsx
import { useRef, useState } from 'react'
import { runFullDemo } from '../demo/runFullDemo'
import { simulate } from '../api/gateway'
import { fetchSnapshot } from '../api/hub'
import { store } from '../stream/store'
import { Panel } from './Panel'

const PROFILES = ['low', 'moderate', 'high', 'overload'] as const

export function DemoControl() {
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const abort = useRef(false)

  async function run(profile: string): Promise<void> {
    setBusy(true)
    setError(null)
    try {
      await simulate(profile)
    } catch (cause) {
      setError(String(cause))
    } finally {
      setBusy(false)
    }
  }

  async function runFull(): Promise<void> {
    setBusy(true)
    abort.current = false
    setError(null)
    const result = await runFullDemo({
      simulate: profile => run(profile).then(() => undefined),
      snapshot: fetchSnapshot,
      onPhase: (name, index, total) => store.setPhase({ name, index, total }),
      shouldStop: () => abort.current,
    })
    store.setPhase(null)
    if (result === 'aborted') setError('demo aborted')
    setBusy(false)
  }

  return (
    <Panel label="demo control">
      <div className="flex items-center gap-2 text-[10px]">
        {PROFILES.map(profile => (
          <button
            key={profile}
            type="button"
            disabled={busy}
            onClick={() => void run(profile)}
            className="rounded border border-line px-3 py-1.5 uppercase tracking-widest text-muted transition hover:border-up hover:text-up disabled:opacity-40"
          >
            {profile}
          </button>
        ))}
        <button
          type="button"
          disabled={busy}
          onClick={() => void runFull()}
          className="rounded border border-up px-4 py-1.5 uppercase tracking-widest text-up transition hover:bg-up/10 disabled:opacity-40"
        >
          run full demo
        </button>
        {busy && (
          <button type="button" onClick={() => { abort.current = true }} className="text-down uppercase tracking-widest">
            stop
          </button>
        )}
        {error && <span className="text-down">{error}</span>}
      </div>
    </Panel>
  )
}
```

`ChapterSlate.tsx`:

```tsx
import { AnimatePresence, motion } from 'framer-motion'
import { useDashboard } from '../stream/store'

const SUBTITLE: Record<string, string> = {
  low: '20 events · 1 ev/s',
  moderate: '100 events · 10 ev/s',
  high: '500 events · 50 ev/s · alerts armed',
  overload: '2000 events · 200 ev/s · dupes ON · corrupt ON',
}

export function ChapterSlate() {
  const phase = useDashboard(state => state.phase)
  return (
    <AnimatePresence>
      {phase && (
        <motion.div
          key={`${phase.index}-${phase.name}`}
          initial={{ y: -48, opacity: 0 }}
          animate={{ y: 0, opacity: 1 }}
          exit={{ y: -48, opacity: 0 }}
          transition={{ duration: 0.3 }}
          className="fixed inset-x-0 top-0 z-10 bg-up/10 py-2 text-center text-xs tracking-[0.3em] text-up"
        >
          PHASE {phase.index}/{phase.total} — {phase.name.toUpperCase()} · {SUBTITLE[phase.name] ?? ''}
        </motion.div>
      )}
    </AnimatePresence>
  )
}
```

`App.tsx` final:

```tsx
import { useEffect } from 'react'
import { startDashboard } from './app/lifecycle'
import { LatencyChart } from './components/LatencyChart'
import { AdapterWall } from './components/AdapterWall'
import { AlertsFeed } from './components/AlertsFeed'
import { ChapterSlate } from './components/ChapterSlate'
import { DemoControl } from './components/DemoControl'
import { IntegrityPanel } from './components/IntegrityPanel'
import { PipelineFlow } from './components/PipelineFlow'
import { ThroughputChart } from './components/ThroughputChart'
import { Ticker } from './components/Ticker'

export default function App() {
  useEffect(() => startDashboard(), [])
  return (
    <div className="min-h-screen bg-bg p-4 font-mono text-fg">
      <div className="grid grid-cols-[1fr_2fr_1fr] grid-rows-[auto_minmax(0,1fr)_auto_auto] gap-3">
        <div className="col-span-3"><Ticker /></div>
        <PipelineFlow />
        <div className="grid min-h-0 grid-rows-2 gap-3">
          <ThroughputChart />
          <LatencyChart />
        </div>
        <div className="grid min-h-0 grid-rows-2 gap-3">
          <AlertsFeed />
          <IntegrityPanel />
        </div>
        <div className="col-span-3"><AdapterWall /></div>
        <div className="col-span-3"><DemoControl /></div>
      </div>
      <ChapterSlate />
    </div>
  )
}
```

In `App.test.tsx` keep the existing region assertions and add the lifecycle mock so the shell test stays offline (place it right after the imports):

```tsx
vi.mock('./app/lifecycle', () => ({ startDashboard: () => () => {} }))
```

- [ ] **Step 4: Run** — `npm --prefix dashboard test` → PASS; `npm --prefix dashboard run build` → OK.
- [ ] **Step 5: Commit**

```bash
git add dashboard
git commit -m "feat: full mission-control dashboard (ticker, pipeline, wall, feeds, control, slate)"
```

---

### Task 16: Hub serves the build; demo.sh, README, ADR-0004

**Files:**
- Modify: `telemetry-hub/src/main/resources/application.yml`
- Modify: `demo.sh`
- Modify: `README.md`
- Create: `docs/adr/0004-dashboard-toolchain-and-sse-tap.md`
- Modify: `AGENTS.md` (untracked, still keep updated for agents)

**Interfaces:**
- Consumes: `dashboard/dist` (Task 7 build).
- Produces: dashboard at `http://localhost:8082/`; `demo.sh` builds the dashboard when missing; documentation reflects the new surface.

- [ ] **Step 1: Serve the build** — in `application.yml`, under `spring:`, add:

```yaml
  web:
    resources:
      static-locations: classpath:/static, file:dashboard/dist/
```

Run the hub from the repo root (as `demo.sh` does) so the relative `file:` location resolves.

- [ ] **Step 2: demo.sh builds the dashboard** — after the jar preconditions and before "Starting services", add:

```bash
section "Dashboard build"
if [ ! -f "$REPO_ROOT/dashboard/dist/index.html" ]; then
  if command -v npm >/dev/null 2>&1; then
    (cd "$REPO_ROOT/dashboard" && npm ci && npm run build)
  else
    echo "WARNING: npm not found; the dashboard will not be served" >&2
  fi
fi
```

- [ ] **Step 3: ADR-0004** — `docs/adr/0004-dashboard-toolchain-and-sse-tap.md`: context (demo needs a live visual; only a sync read API exists), options considered (server aggregation vs pure forwarding; Grafana; separate frontend service; polling), decision (dashboard/ Vite+React served by the hub as static files; hub SSE tap via `TelemetryTap` port + in-memory frames; client-side aggregation; direct browser→gateway control with CORS allowlist), consequences (Node toolchain joins the repo; hub state stays in-memory and ephemeral by design; contracts stay Java-only).

- [ ] **Step 4: README + AGENTS.md** — README: add the dashboard to the architecture diagram box list, a "Dashboard" quickstart subsection (`npm --prefix dashboard install` once, served automatically at `:8082/`, dev mode `npm --prefix dashboard run dev` with proxy), and testing-table rows (frontend unit: Vitest + Testing Library). AGENTS.md: module table row for `dashboard/` (not a Maven module; commands `npm --prefix dashboard test|run build`; rule: hub API same-origin, gateway only via `VITE_GATEWAY_URL`).

- [ ] **Step 5: Verify manually** — `npm --prefix dashboard run build`, start the hub from the repo root, then:

```bash
curl -sf http://localhost:8082/ | grep -o '<title>[^<]*</title>'
curl -sf http://localhost:8082/api/v1/metrics/snapshot
curl -sN --max-time 2 http://localhost:8082/api/v1/stream | head -4
```

Expected: the index HTML; snapshot JSON; `event:heartbeat`/retry preamble lines from the stream (or `retry:` — any SSE framing proves the endpoint).

- [ ] **Step 6: Commit**

```bash
git add telemetry-hub/src/main/resources/application.yml demo.sh README.md docs/adr/0004-dashboard-toolchain-and-sse-tap.md
git commit -m "docs+feat: hub serves dashboard build; demo.sh builds it; ADR-0004"
```

---

### Task 17: Acceptance — the demo, end to end

**Files:** none (verification only). Preconditions: Docker running, `mvn -q package` green, dashboard built.

- [ ] **Step 1: Full build** — `mvn -q package` → BUILD SUCCESS (surefire runs unit + slice + `TelemetryListenerIT` including the DLT test).
- [ ] **Step 2: Frontend** — `npm --prefix dashboard test && npm --prefix dashboard run build` → green.
- [ ] **Step 3: Run the verified demo** — `docker compose up -d --wait`, then `./demo.sh`. During the run, keep `http://localhost:8082/` open and watch: throughput climbs per phase; chapter slates sweep on phase change (dashboard-initiated runs only — launched from `demo.sh` there is no phase marker, that is per spec); after `overload` the duplicates and DLT counters are non-zero.
- [ ] **Step 4: Spec acceptance §10** — check each item:
  1. all six views show live data at `http://localhost:8082/`;
  2. running `overload` from `DemoControl`: throughput ~200 ev/s, duplicates/DLT move, Oracle `SELECT COUNT(*) FROM telemetry_event` unchanged across re-run;
  3. a `high` run pulses exactly one in-place alert per 3-DOWN episode (red glow, neighbours dim);
  4. kill the hub (`pkill -f telemetry-hub`, restart via `java -jar …`): Ticker shows RECONNECTING → LIVE, totals return correct, throughput shows the red reset marker;
  5. reduced-motion OS setting collapses animations to color-only.
- [ ] **Step 5: Commit any stragglers**

```bash
git status --porcelain   # everything already committed; fix leftovers if any
```

---

## Self-review notes (kept for the executor)

- Spec coverage: §4 backend (Tasks 1–6), §4.2 frontend structure (Tasks 7–15), §5 contract (Tasks 3–4, 9–11), §6 visuals (Tasks 8, 14–15), §7 resilience (Tasks 4, 9, 11, 15: reconnect → gap → snapshot → reset marker; slow-client caps live in store/ServerEmitters), §8 testing levels (each task), §9 docs (Task 16), §10 acceptance (Task 17).
- Deliberate deviation from spec §5.2 wording: `totals` comes from hub memory (duplicates/alerts/dlt of the current hub session), not Oracle — Oracle has no counters for duplicates/DLT; the DB remains the idempotency referee exactly as before. Documented here so nobody "fixes" it into a lie.
- `seq` on the wire is the hub's monotonic frame counter; heartbeats reuse `seq()` without incrementing, so gap detection only ever fires on genuinely missed frames.
- Slow-client handling simplification vs spec §7: `SseEmitter.send` is synchronous, so a stuck client is evicted on send failure (`catch` → `emitters.remove`) rather than via an explicit buffer-limit knob, which Spring's SseEmitter does not expose. With one or two demo clients this is equivalent in practice; revisit only if a real multi-client deployment appears.

