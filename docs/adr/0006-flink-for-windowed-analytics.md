# ADR-0006: Apache Flink for windowed analytics — evaluated, not adopted

Date: 2026-11-16 · Status: Accepted (decision: **do not adopt now**; shape recorded for a future trigger)

## Context

The pipeline is a genuine streaming system: the gateway publishes `TelemetryEvent`
JSON with an event-time field (`occurred_at`) to `adapter.telemetry.v1`, and the hub
consumes it, upserts idempotently into Oracle, tracks per-adapter health and raises
alerts. Two of its concerns are classic stream-processing problems:

- **Windowed KPI.** Throughput buckets and latency percentiles (`p50/p95/p99`) are
  computed **client-side** by the dashboard from SSE frames
  (`dashboard/src/stream/series.ts`). That choice is deliberate (ADR-0004) and fine
  for a demo, but it is fragile as a product feature: a browser refresh loses the
  window, there is no event-time/watermark handling for out-of-order or redelivered
  events (the DLT retry path redelivers late by construction), and durable windowed
  analytics would otherwise have to be hand-rolled in the hub.
- **Keyed state.** The DOWN-streak logic (`AdapterHealth.observe`) is keyed state
  over a stream, currently held in memory by the hub and re-derived nothing — a
  restart loses it (the alert id is deterministic, so no correctness bug, but no
  recovery either).

Apache Flink is the standard answer to both: event-time windows with watermarks,
keyed state with checkpoints, exactly-once sinks, and CEP for multi-event alert
patterns. It is also a heavy answer — a JobManager plus at least one TaskManager
(a couple of GB of resident memory), a new runtime (fat-jar jobs, no Spring, its
own idioms) and operational surface (checkpoints, savepoints, state backends) — on
a homelab that already runs Kafka, Oracle, llama.cpp and Strata, in a repo whose
stated purpose is a *readable, interview-grade hexagonal Java codebase*.

## Options considered

- **Flink replaces the hub consumer.** The hub's `TelemetryListener` →
  `ProcessTelemetryUseCase` flow becomes a Flink job; Flink owns state and the
  exactly-once story. Rejected: it would delete the very things this repo exists to
  show (hexagonal ports/adapters, the `Result` discipline, idempotency-by-primary-
  key as the referee per ADR-0002/0003, Tell-Don't-Ask aggregates), and it swaps a
  solved correctness model (deterministic ids + Oracle PK) for a different one
  (checkpoints + two-phase commit) at demo scale where the former cannot fail.
  Exactly-once and idempotency would then coexist without either being trusted.
- **Flink as an additive analytics layer (the shape if ever adopted).** A separate,
  optional compose profile runs a small job that reads `adapter.telemetry.v1` and
  writes windowed KPIs to a new topic (e.g. `adapter.metrics.v1`); the dashboard
  reads them alongside the existing SSE tap. The hub's correctness path stays
  untouched; `contracts` stays dependency-free (Flink never enters it).
  Defeasible on cost, not on correctness — this is the only shape worth taking.
- **Flink SQL as a scratchpad.** Query the topic ad hoc without owning state.
  Cheapest Flink entry, but solves nothing the read API + Oracle don't already
  answer, so it would be tooling for its own sake.
- **Status quo: client-side windows + in-memory tap.** What ships today. Sufficient
  while the audience is a browser during `demo.sh` and the traffic source is the
  seeded simulator.

## Decision

**Do not adopt Flink now.** Record the evaluation so the question is not relitigated
from scratch: the acceptable future shape is *additive and isolated* — an optional
profile with its own topic, never in the hub's processing path, never inside
`contracts`, and only after a trigger below fires. Any adoption requires a new ADR
superseding this one.

## Consequences

- Windowed KPIs stay client-side and ephemeral: a refresh resets them; the durable
  numbers are whatever Oracle holds. Accepted for a demo.
- `AdapterHealth` state stays in the hub's memory: a restart clears streaks, but
  the deterministic alert id makes the resulting behaviour safe, not wrong.
- Homelab footprint, build time and the demo's `docker compose up -d --wait` path
  are unchanged — no cluster, no new JVM toolchain in CI.
- The repo keeps one story: two Spring services, one contracts module, one
  dashboard. The ADR list gains a "we considered streaming processing and the
  threshold for it is *this*, not vibes".
- Cost of the deferral: if a real windowed-analytics need appears before this is
  revisited, it will be built the hand-rolled way first (likely in the hub, as a
  port + infrastructure implementation) and may then need migrating.

## When to revisit

Any one of these is a sufficient trigger:

- **Durable, server-side windowed analytics become a feature** (p95 per adapter
  over minutes, surviving refreshes and restarts) *and* the hand-rolled version in
  the hub would be the second implementation of the same logic.
- **Out-of-order/late event handling stops being acceptable to ignore** — e.g. the
  demo (or a real deployment) starts replaying history against live windows.
- **A multi-event correlation requirement appears** («DOWN + WARN within 30 s»
  across adapters) that reads as CEP rather than as an aggregate.
- **Traffic volume or consumer count outgrows one Spring listener** — measurable,
  not hypothetical: sustained rates where the hub's in-memory tap and single
  consumer lag behind the simulator's `high`/`overload` profiles.
- **The homelab gains headroom** (fixed memory budget for a cluster profile) so
  JobManager + TaskManager stop competing with the model servers.

When a trigger fires: optional `flink` compose profile, job reading
`adapter.telemetry.v1`, writing `adapter.metrics.v1`, dashboard as an additional
consumer — and a new ADR that supersedes this one.
