import { useSyncExternalStore } from 'react'
import { countEvent, emptySeries, foldTelemetry, markReset, type Series } from './series'
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
    const gap = frame.seq > this.state.seq + 1
    let series = this.state.series
    if (gap) series = markReset(series, Date.now())
    const patch: Partial<DashboardState> = { seq: frame.seq, series }
    switch (frame.kind) {
      case 'telemetry':
        patch.sessionEvents = this.state.sessionEvents + 1
        patch.series = foldTelemetry(series, frame.latencyMs, Date.now())
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
        patch.series = countEvent(series, Date.now())
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

/**
 * Subscribes a component to the dashboard store.
 * The selector must return primitives or stable references (arrays/objects
 * held by the state, not freshly-built ones) or the component re-renders on
 * every notification.
 */
export function useDashboard<T>(selector: (state: DashboardState) => T): T {
  return useSyncExternalStore(store.subscribe, () => selector(store.getState()))
}
