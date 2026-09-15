# ADR-0004: Dashboard toolchain and the SSE tap

Date: 2026-09-15 · Status: Accepted

## Context

The demo tells its story best when the audience can watch it: events flowing,
duplicates being rejected, alerts firing, dead letters accumulating. Until now the
hub exposed only a synchronous read API (`GET /api/v1/adapters`, `GET
/api/v1/adapters/{id}`), which shows the *result* of a run but not the run itself —
there is nothing live to project on a screen while `demo.sh` executes. Adding a
visual layer raises three questions at once: where does aggregation happen (server
or client), what serves the frontend, and how does the browser learn about events
(polling or push).

## Options considered

- **Server-side aggregation + polling.** The hub computes rolling windows
  (throughput, latency percentiles) and the frontend re-fetches a summary every
  second. No push infrastructure, but per-poll work in the hub, quantization
  artifacts between polls, and a chatty REST surface to maintain — for a demo that
  mostly needs forwarding, not computing.
- **Grafana + a metrics exporter.** Zero frontend code and production-grade
  panels, but it drags a second server, a datasource and a dashboard-as-config
  workflow into a repo whose point is a readable hexagonal Java codebase. It also
  cannot drive the demo (trigger profiles) or present the pipeline in the
  project's own visual language.
- **Separate frontend service.** A dedicated static server (nginx, or Vite dev
  server in production too) keeps concerns tidy, but adds a third port, third
  process and a CORS story to every data call — deployment complexity the demo
  does not need.
- **Push vs poll for live data.** Server-sent events fit the shape of the data
  exactly: an ordered, append-only stream of small frames flowing one way, with
  free browser reconnection. WebSockets would buy bidirectionality the dashboard
  never uses; polling was rejected above.

## Decision

A single-page **dashboard** app in `dashboard/` (Vite + React + TypeScript,
Tailwind, ECharts), served by the telemetry-hub itself as static files: the hub
adds `file:dashboard/dist/` to `spring.web.resources.static-locations`, so the
built bundle and the read API share the origin `http://localhost:8082`. This
requires running the hub from the repo root (as `demo.sh` does) so the relative
`file:` location resolves.

For live data the hub grows a read-side **SSE tap**: the `TelemetryTap` port
(application) is notified by `ProcessTelemetryUseCase` of every decision —
processed, duplicate, alert — and `InMemoryTelemetryMetrics` (infrastructure)
turns each into a monotonically sequenced immutable `Frame` that
`MetricsStreamController` fans out as named SSE events on `GET /api/v1/stream`,
with `GET /api/v1/metrics/snapshot` for (re)connect state and a 15 s heartbeat
keep-alive. `DltObserver` listens to the DLT topic and feeds dead-letter frames
into the same stream. The controller is a pure forwarder: **aggregation happens
client-side** (throughput and latency series are built in the browser from the
frames). The tap is fire-and-forget by contract — implementers must be fast and
non-throwing, so observability can never slow down or fail the pipeline.

The dashboard controls the demo by calling the gateway directly from the browser
(`POST /api/v1/telemetry/simulate?profile=…`), with the gateway's CORS
configuration allowing exactly one origin: `http://localhost:8082` (the hub, which
is where the dashboard is served from). Data never crosses origins — it is
same-origin by construction; only the demo control POST does, and it is
allowlisted. Outside the browser, the gateway base URL defaults to
`http://localhost:8081` and can be overridden at build time with
`VITE_GATEWAY_URL`.

## Consequences

- **A Node toolchain joins the repo.** Building and testing the dashboard
  requires npm; `demo.sh` builds `dashboard/dist/` on demand (skipped when
  present, warning only when npm is missing) so the Java-only happy path still
  works. This is the repo's first non-Java, non-Maven module — it is not part of
  the Maven reactor.
- **Hub metrics state is in-memory and ephemeral by design.** Restarting the hub
  resets sequence numbers and counters; the snapshot endpoint restores cumulative
  context within a session, not across restarts. Durable analytics belong to
  Oracle and the existing read API, not to the tap.
- **Contracts stay Java-only.** The dashboard consumes the same JSON the services
  exchange, but no shared types are generated for TypeScript; the frontend's
  `types.ts` is a hand-written, test-backed mirror of the contract shapes.
- **Same-origin serving avoids CORS for data entirely.** Only the browser→gateway
  control POST crosses origins, and the CORS surface is a single pinned origin —
  nothing wildcarded.
- The SSE endpoint is an unauthenticated read side, like the rest of the read
  API; acceptable for a demo running on localhost.

## When to revisit

If the dashboard must survive hub restarts, persist tap state (or rebuild it from
Oracle) before treating seq/counters as meaningful. If anything beyond the demo
control POST needs to cross origins, replace the single pinned origin with a
config-driven allowlist — and re-examine whether that logic belongs in the
gateway at all. If a second consumer of the live stream appears (CLI, alerts
relay), promote the frames to a contract-level schema instead of the hub-internal
sealed `Frame` types.
