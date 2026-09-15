export type Status = 'UP' | 'DEGRADED' | 'DOWN'

export interface Totals {
  duplicates: number
  alerts: number
  dlt: number
}

export interface Snapshot {
  seq: number
  totals: Totals
}

export type StreamFrame =
  | { kind: 'telemetry'; seq: number; eventId: string; adapterId: string; status: Status; latencyMs: number; country: string; occurredAt: string }
  | { kind: 'duplicate'; seq: number; eventId: string; adapterId: string }
  | { kind: 'alert'; seq: number; alertId: string; adapterId: string; reason: string; raisedAt: string; triggerEventId: string }
  | { kind: 'dlt'; seq: number; reason: string }
  | { kind: 'heartbeat'; seq: number; totals: Totals }

export interface AdapterSummary {
  adapterId: string
  consecutiveDown: number
  alertActive: boolean
  lastSeen: string
}

export type ConnectionStatus = 'connecting' | 'live' | 'reconnecting'
