-- Telemetry pipeline schema. Idempotency is enforced by the database:
-- EVENT_ID is the primary key, ALERT_ID is derived deterministically from the
-- event that triggered the alert (see AlertEvent.forTrigger).

CREATE TABLE telemetry_event (
  event_id    RAW(16)       PRIMARY KEY,
  adapter_id  VARCHAR2(40)  NOT NULL,
  country     VARCHAR2(2)   NOT NULL,
  status      VARCHAR2(10)  NOT NULL,
  latency_ms  NUMBER(6)     NOT NULL CHECK (latency_ms >= 0),
  occurred_at TIMESTAMP(3)  NOT NULL,
  ingested_at TIMESTAMP(3)  DEFAULT SYSTIMESTAMP NOT NULL
);

CREATE TABLE adapter_health (
  adapter_id       VARCHAR2(40) PRIMARY KEY,
  consecutive_down NUMBER(3)    NOT NULL CHECK (consecutive_down >= 0),
  alert_active     NUMBER(1)    NOT NULL CHECK (alert_active IN (0, 1)),
  last_seen        TIMESTAMP(3) NOT NULL
);

CREATE TABLE adapter_alert (
  alert_id  RAW(16)       PRIMARY KEY,
  adapter_id VARCHAR2(40) NOT NULL,
  reason     VARCHAR2(200) NOT NULL,
  raised_at  TIMESTAMP(3)  NOT NULL
);

CREATE INDEX idx_telemetry_adapter ON telemetry_event (adapter_id, occurred_at);
CREATE INDEX idx_telemetry_ingested ON telemetry_event (ingested_at);
