-- Restores the spec schema (plan §5 / Task 7): every alert records the telemetry
-- event that triggered it, and the mapping trigger -> alert is 1:1 — enforced by
-- the database, matching the deterministic AlertEvent.forTrigger derivation.
-- Applied on the from-zero path (V1 -> V2, empty table), so NOT NULL needs no default.
ALTER TABLE adapter_alert ADD (
  trigger_event_id RAW(16) NOT NULL
);

ALTER TABLE adapter_alert ADD CONSTRAINT uq_adapter_alert_trigger UNIQUE (trigger_event_id);
