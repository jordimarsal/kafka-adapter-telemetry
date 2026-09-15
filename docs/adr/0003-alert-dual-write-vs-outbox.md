# ADR-0003: Alert dual-write vs transactional outbox

Date: 2026-09-15 · Status: Accepted (assumed trade-off, documented)

## Context

When the hub observes the third consecutive DOWN for an adapter it must do two
things in the same instant: persist the alert row in Oracle (the read API queries
it) and publish an `AlertEvent` to the `adapter.alerts.v1` topic for other systems.
These are two different systems — a database transaction and a Kafka publish — so
there is no atomic commit spanning both. The textbook fix is the transactional
outbox pattern: write the alert to an outbox table inside the same transaction and
let a relay (poller or Debezium) publish it to Kafka. For this demo that means a
fourth table, a relay process with its own delivery guarantees, and more moving
parts to explain — chosen against the reality that alerts are derived state that
replays can reconstruct (alert ids are deterministic: `AlertEvent.forTrigger`
derives the id from the triggering event id, so a replayed trigger never creates a
second alert row).

## Decision

Accept the dual-write: within processing one event, the hub commits the alert to
Oracle first and then publishes to Kafka, without an outbox. The trade-off is
explicit: the two writes are not atomic, so a crash between them can leave an alert
visible in the database but never published (or, on redelivery after a partial
failure, published twice — the deterministic alert id and Oracle's primary key keep
the database side deduplicated either way). Telemetry events themselves do not have
this problem: idempotent upsert by `event_id` makes replay harmless.

## Consequences

- Simple, synchronous flow: one method, one transaction, one publish; easy to test
  end to end (this demo's whole point).
- Kafka is not the source of truth: Oracle is. Anyone treating
  `adapter.alerts.v1` as a guaranteed exactly-once feed will observe rare loss or
  duplication on crashes — acceptable for alert notifications, unacceptable for
  billing-style events.
- The relay logic (retry, ordering, cleanup of published outbox rows) simply does
  not exist and therefore cannot fail — or help — yet.

## When to revisit

The moment a consumer of `adapter.alerts.v1` must not miss or duplicate alerts
(paging, automated remediation), introduce the transactional outbox: outbox table
written in the same Oracle transaction, relayed by a scheduled publisher or
Debezium. The deterministic alert id already in place makes that migration
backwards-compatible.
