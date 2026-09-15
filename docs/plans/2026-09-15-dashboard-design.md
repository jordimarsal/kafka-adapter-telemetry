# Design — Live Demo Dashboard (`dashboard/`)

- **Date:** 2026-09-15
- **Status:** approved design, pending implementation plan
- **Follows:** brainstorming session of 2026-09-15 (decisions recorded below)
- **Related:** ADR-0004 to be written with this (Node/React build toolchain + SSE choice)

## 1. Context and problem

The demo (`demo.sh`) is currently terminal-driven: services start, profiles run,
SQL counts prove idempotency. Nothing *shows* the pipeline living. We need a
screen to present on stage/interviews that visualises the flow in real time —
clear, useful, and visually exceptional.

Today the only data surface is the synchronous read API
(`GET /api/v1/adapters[/{id}]`, last-20 lists). There is no push channel, no
aggregate endpoint, and no CORS configuration.

## 2. Goals / non-goals

**Goals**

1. One-screen Mission-Control dashboard showing the demo from ingest to Oracle,
   live, for all six views: throughput + demo phases, adapter health wall,
   live alerts, idempotency + DLT, latency distribution, animated pipeline.
2. Mixed fidelity: per-event stream for wow moments (alerts, feed), client-side
   aggregates for charts (1 s buckets, p50/p95).
3. The dashboard is also the **demo controller**: buttons fire profiles at the
   gateway and a "Run full demo" sequences low → high → overload ×2.
4. Single deployable for the demo: the Vite build is served by the hub as
   static content (same origin, no CORS for data, one JAR).

**Non-goals**

- No persistence of metrics (in-memory windows are ephemeral by design; Oracle
  keeps the cumulative truth it already keeps).
- No Grafana/Prometheus ops view (possible future companion, not now).
- No multi-user/auth concerns (local demo), no mobile layout (stage laptop),
  no historical dashboards beyond the current run + cumulative totals.

## 3. Decisions taken (brainstorm log)

| Question | Decision |
|---|---|
| Live fidelity | Mixed: per-event SSE + client-side aggregation |
| Views | All six (throughput+phases, wall, alerts, integrity, latency, pipeline) |
| Stack & location | Vite + React + TS + Tailwind + ECharts + framer-motion in `dashboard/` (outside Maven) |
| Data architecture | **A**: hub as single source — `TelemetryTap` port + in-memory metrics + SSE; DLT observer consumer |
| Demo control | Dashboard commands profiles directly against the gateway (CORS allowlist) |
| Layout | **Mission Control** grid, with cinematic *chapter slates* on phase change |
| Aesthetic | **Terminal Noir** — mono type, 1px hairlines, near-black, restrained phosphor green |
| Alert emphasis | **In-place Pulse** — card scales ~1.07 + glow, neighbours dim; no overlay |

## 4. Architecture

```
dashboard/ (Vite+React+TS)  --dev--> Vite proxy --> telemetry-hub :8082
        └-- prod: npm run build --> hub serves dashboard/dist as static
                                          │
              ┌───────────────────────────┼──────────────────────────┐
    application/                 infrastructure/                 new infra:
    port TelemetryTap <────────── InMemoryTelemetryMetrics
    (use case notifies            + DltObserver (own group on *.dlt)
     processed/duplicate/alert)   + MetricsStreamController
                                    ├─ GET /api/v1/metrics/snapshot
                                    └─ GET /api/v1/stream        (SSE)
gateway :8081  ◀── control POSTs directly from the browser (CORS allowlist)
```

### 4.1 Backend changes (telemetry-hub)

| Piece | Kind | Detail |
|---|---|---|
| `application/TelemetryTap` | new port (pure interface) | `onProcessed(TelemetryEvent)`, `onDuplicate(TelemetryEvent)`, `onAlert(AlertEvent)` — same pattern as `AlertPublisher` |
| `ProcessTelemetryUseCase` | +1 dependency | notifies the tap at the three points it already has; wired in `UseCaseConfig`; tests use a recording **fake** |
| `infrastructure/metrics/InMemoryTelemetryMetrics` | adapter | implements the tap; monotonic `seq`, sliding windows (throughput, latency), capped feed buffer; thread-safe (single writer, many SSE readers) |
| `infrastructure/metrics/DltObserver` | adapter (Kafka) | own consumer group on `adapter.telemetry.v1.dlt`; counts messages, feeds the metrics registry; always counts, never blocks the main flow |
| `infrastructure/readapi/MetricsStreamController` | controller | `GET /api/v1/stream` SSE broadcast (registry of `SseEmitter`s, heartbeat 15 s) + `GET /api/v1/metrics/snapshot` |
| `UseCaseConfig` / config | wiring | tap bean is the in-memory implementation (always on) |

**Deliberate constraints:** no new tables; no dependency hub→gateway (control
goes browser→gateway); aggregates in memory are ephemeral — the snapshot
restores cumulative counters from Oracle (counts the stores already provide).

### 4.2 Frontend (`dashboard/`)

```
dashboard/src/
├── app/          # shell, theme, Mission-Control grid
├── stream/       # StreamClient (EventSource + reconnect→snapshot),
│                 # external store via useSyncExternalStore (zero deps),
│                 # per-event buffers + rAF flush (~10 Hz for charts)
├── api/          # snapshot, adapters, control (POST :8081) clients
├── components/
│   ├── PipelineFlow/    # animated ingest→Kafka→hub→Oracle with live counters
│   ├── ThroughputChart/ # timeseries + phase markers + "series reset" line
│   ├── LatencyChart/    # 1 s-bucket histogram + p50/p95 lines
│   ├── AdapterWall/     # per-adapter cards (status, streak, last seen)
│   ├── AlertsFeed/      # live alerts (cap 50), pulse on arrival
│   ├── IntegrityPanel/  # duplicates rejected + DLT counters
│   └── DemoControl/     # profile buttons, phase state, "Run full demo"
└── design/       # tokens, primitives (GlowCard, StatusDot, Ticker), motion presets
```

Key mechanics:

- **No state library.** Events fold into mutable buffers; one rAF flusher
  derives series (1 s buckets, histogram, percentiles) and notifies subscribers;
  components subscribe via selectors. ECharts updates through `setOption`
  outside React; framer-motion handles card/feed transitions.
- **Pure, testable core:** event→series folding, seq-gap detection and the
  "Run full demo" state machine (wait for counter stabilisation, like
  `demo.sh` does) are pure functions — Vitest without DOM/server.

## 5. Data contract

### 5.1 `GET /api/v1/stream` — `text/event-stream`

| SSE event | Payload (JSON) | Source | Consumer |
|---|---|---|---|
| `telemetry` | `{seq, eventId, adapterId, status, latencyMs, country, occurredAt}` | tap | throughput, histogram, wall, feed |
| `duplicate` | `{seq, eventId, adapterId}` | tap (`Result.Err` path) | integrity panel |
| `alert` | full `AlertEvent` (existing contract) | tap | alerts feed + wall flash |
| `dlt` | `{seq, reason}` | DltObserver | integrity panel |
| `heartbeat` | `{seq, totals}` every 15 s | broadcaster | totals resync, keep-alive |

- `seq` is monotonic across all events; the client detects gaps and, on
  reconnect, refetches the snapshot (no `Last-Event-ID` replay — YAGNI).
- The server does **not** aggregate: it forwards what it already knows; the
  client buckets in rAF. At 200 ev/s this is ~30 KB/s of small JSON.

### 5.2 `GET /api/v1/metrics/snapshot`

```json
{ "totals": { "processed": 2365, "duplicates": 95, "alerts": 14, "dlt": 20 },
  "generatedAt": "2026-09-15T10:12:30Z" }
```

Cumulative counters come from Oracle. The adapter wall initialises from the
existing `GET /api/v1/adapters`. On load or reconnect the client calls
snapshot once; live series start empty (documented behaviour, not a bug).

## 6. Visual specification (Terminal Noir)

Tokens (CSS custom properties in `design/tokens.css`):

| Token | Value | Use |
|---|---|---|
| `--bg` | `#05070a` | page background |
| `--panel` | `#0a0e14` | cards/panels |
| `--line` | `#1c2530` | 1px hairlines |
| `--text` | `#e6edf3` | primary text / big numbers |
| `--muted` | `#8b98ab` | secondary text |
| `--dim` | `#4d5a6d` | labels (uppercase, tracked) |
| `--up` | `#3ddc97` | UP status, positive, live |
| `--warn` | `#ffb347` | DEGRADED, reconnecting |
| `--down` | `#ff5470` | DOWN, alerts, DLT, errors |

Type: JetBrains Mono (or `ui-monospace` fallback) everywhere; big numerals
`clamp()`-scaled; uppercase micro-labels with +1px tracking. Subtle glow only
on live numbers and status dots (`text-shadow: 0 0 18px rgba(61,220,151,.35)`).
Respect `prefers-reduced-motion` (transitions become color-only).

Layout (Mission Control):

```
┌──────────────────────────────────────────────────────────────┐
│ TICKER  ▶ 2,365 events · 14 alerts · 20 DLT · PHASE: OVERLOAD│
├────────────┬──────────────────────────────┬──────────────────┤
│ PIPELINE   │ THROUGHPUT ev/s (+ phases)   │ ALERTS (live)    │
│ (animated  ├──────────────────────────────┼──────────────────┤
│  flow with │ LATENCY histogram + p50/p95  │ INTEGRITY        │
│  counters) │                              │ dupes · DLT      │
├────────────┴──────────────────────────────┴──────────────────┤
│ ADAPTER WALL  [gw-es-1] [gw-de-2] [gw-es-3] …                │
├──────────────────────────────────────────────────────────────┤
│ DEMO CONTROL  [low] [moderate] [high] [overload] [RUN FULL]  │
└──────────────────────────────────────────────────────────────┘
```

Motion:

- **In-place Pulse (alerts):** card `scale(1.07)` + red glow ~1.5 s in/out;
  neighbours dim to opacity .45; status dot pulses. No overlay ever.
- **Chapter slate (phase change):** a header strip sweeps in with mono text
  `PHASE 04 — OVERLOAD · 200 ev/s · dupes ON · corrupt ON`, holds ~1.2 s,
  sweeps out; throughput chart drops a permanent phase marker line. Phase
  detection is client-side: markers exist only for dashboard-initiated runs
  (the backend has no concept of phases; `demo.sh` runs get no markers).
- Feed arrivals slide+fade (framer-motion); numbers tick via text transitions.

## 7. Resilience

| Situation | Behaviour |
|---|---|
| SSE drops | browser auto-reconnects; client shows `RECONNECTING` chip (amber); on reopen, seq gap → refetch snapshot; heartbeat resyncs totals; `lastSeq` dedupes |
| Hub restart mid-demo | client backoff 1→2→4…s (cap 10 s); snapshot restores cumulative; in-memory series restart → "series reset" marker line on throughput |
| Slow client / hidden tab | buffers capped (feed 50, series 10 min of 1 s buckets); rAF pauses and flushes on visibility; server `SseEmitter` buffer limit drops the slow client rather than blocking broadcast |
| Gateway down (control) | POST failure → error state in `DemoControl` (red, retry); "Run full demo" state machine aborts cleanly and reports the last completed phase |

## 8. Testing (TDD, per level)

| Level | What | Tooling |
|---|---|---|
| Hub unit | `InMemoryTelemetryMetrics`: seq monotonicity, ring-buffer caps, totals with fake events; use-case tap notifications (recording fake) | JUnit 5, no Spring |
| Hub web slice | `/metrics/snapshot` shape and cumulative counts | `@WebMvcTest` |
| Hub Kafka IT | `DltObserver` counts messages published to the DLT | `@EmbeddedKafka` (runs in `mvn package`) |
| FE unit | event→series folding (buckets, histogram, p50/p95), seq-gap detection, Run-full-demo state machine | Vitest, pure functions |
| FE component | `DemoControl` fires correct profile POST; error states | Vitest + Testing Library |
| E2E | none (YAGNI) — the live demo is the E2E | — |

## 9. Documentation obligations

- **ADR-0004** — frontend build toolchain (Node/Vite/React) as the repo's first
  non-Java dependency, and SSE-vs-polling-vs-Grafana decision record.
- **README** — architecture diagram gains the dashboard; testing table gains
  Vitest rows; quickstart gains `dashboard/` dev/build commands.
- **AGENTS.md** — new module row in the modules table (`dashboard/` is not a
  Maven module but needs a rule set: package manager, build, test commands).

## 10. Acceptance criteria

1. `docker compose up -d --wait`, `mvn -q package`, hub running, dashboard
   served at `http://localhost:8082/` shows all six views with live data.
2. Running `overload` from the dashboard: throughput chart climbs to ~200 ev/s,
   duplicates and DLT counters move in real time, the row count in Oracle does
   not change across a re-run (visible in the integrity panel).
3. A `high` run pulses exactly one in-place alert per 3-DOWN episode.
4. Killing and restarting the hub mid-demo shows RECONNECTING → recovery with
   correct cumulative totals and a series-reset marker.
5. `mvn -q package` stays green including the new hub unit/slice/Kafka ITs;
   `npm test` passes in `dashboard/`.

## 11. Out of scope / future

Prometheus/Grafana ops view; mobile; auth; historical (cross-run) series;
consumer-lag visualisation (needs micrometer registry — revisit with ADR-0004
follow-up if wanted).
