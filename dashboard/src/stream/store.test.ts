import { beforeEach, describe, expect, test, vi } from 'vitest'
import { DashboardStore } from './store'
import type { StreamFrame } from './types'

const fresh = () => new DashboardStore()

const telemetry = (seq: number, latencyMs = 100): StreamFrame =>
  ({ kind: 'telemetry', seq, eventId: `e${seq}`, adapterId: 'gw-1', status: 'UP', latencyMs, country: 'ES', occurredAt: '2026-09-15T10:00:00Z' })

describe('store.apply', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-09-15T10:00:30Z'))
  })

  test('counts session events and folds telemetry into the series', () => {
    const s = fresh()
    s.apply(telemetry(1))
    s.apply(telemetry(2))
    s.flush()
    const state = s.getState()
    expect(state.sessionEvents).toBe(2)
    expect(state.series.buckets.at(-1)?.count).toBe(2)
    expect(state.seq).toBe(2)
  })

  test('ignores out-of-order frames and marks a reset on gaps', () => {
    const s = fresh()
    s.apply(telemetry(1))
    s.apply(telemetry(5))
    s.flush()
    const state = s.getState()
    expect(state.seq).toBe(5)
    expect(state.series.resets).toHaveLength(1)
  })

  test('drops stale frames without changing any visible state', () => {
    const s = fresh()
    s.apply(telemetry(5))
    s.apply(telemetry(2))
    s.flush()
    const state = s.getState()
    expect(state.seq).toBe(5)
    expect(state.sessionEvents).toBe(1)
    expect(state.alertsFeed).toHaveLength(0)
    expect(state.dltFeed).toHaveLength(0)
  })

  test('duplicate frames count as pipeline traffic but add no latency samples', () => {
    const s = fresh()
    s.apply(telemetry(1, 120))
    s.apply({ kind: 'duplicate', seq: 2, eventId: 'e1', adapterId: 'gw-1' })
    s.flush()
    const state = s.getState()
    expect(state.series.buckets.at(-1)?.count).toBe(2)
    expect(state.series.buckets.at(-1)?.samples).toEqual([120])
    expect(state.sessionEvents).toBe(1)
  })

  test('alerts and dlt frames feed their lists with caps', () => {
    const s = fresh()
    for (let i = 1; i <= 55; i++) {
      s.apply({ kind: 'alert', seq: i, alertId: `a${i}`, adapterId: 'gw-1', reason: '3 DOWN', raisedAt: '2026-09-15T10:00:00Z', triggerEventId: `e${i}` })
    }
    s.apply({ kind: 'dlt', seq: 100, reason: 'broken' })
    s.flush()
    expect(s.getState().alertsFeed).toHaveLength(50)
    expect(s.getState().alertsFeed[0].alertId).toBe('a55')
    expect(s.getState().dltFeed[0].reason).toBe('broken')
  })

  test('snapshot and heartbeat refresh seq and totals', () => {
    const s = fresh()
    s.applySnapshot({ seq: 9, totals: { duplicates: 3, alerts: 1, dlt: 2 } })
    s.apply({ kind: 'heartbeat', seq: 11, totals: { duplicates: 4, alerts: 1, dlt: 3 } })
    s.flush()
    const state = s.getState()
    expect(state.seq).toBe(11)
    expect(state.totals).toEqual({ duplicates: 4, alerts: 1, dlt: 3 })
  })

  test('notifies subscribers once per flush, not per apply', () => {
    const s = fresh()
    const listener = vi.fn()
    const unsubscribe = s.subscribe(listener)
    s.apply(telemetry(1))
    s.apply(telemetry(2))
    s.apply(telemetry(3))
    expect(listener).not.toHaveBeenCalled()
    s.flush()
    expect(listener).toHaveBeenCalledTimes(1)
    unsubscribe()
  })
})

describe('store.reset', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-09-15T10:00:30Z'))
  })

  test('clears session state and adopts the fresh snapshot', () => {
    const s = fresh()
    s.apply(telemetry(1))
    s.apply({ kind: 'alert', seq: 2, alertId: 'a1', adapterId: 'gw-1', reason: '3 DOWN', raisedAt: 'x', triggerEventId: 'e1' })
    s.apply({ kind: 'dlt', seq: 3, reason: 'broken' })
    s.setAdapters([{ adapterId: 'gw-1', consecutiveDown: 0, alertActive: false, lastSeen: 'x' }])
    s.setPhase({ name: 'low', index: 1, total: 4 })
    s.flush()

    s.reset({ seq: 9081, totals: { duplicates: 0, alerts: 0, dlt: 0 } })
    s.flush()
    const state = s.getState()
    expect(state.seq).toBe(9081)
    expect(state.totals).toEqual({ duplicates: 0, alerts: 0, dlt: 0 })
    expect(state.sessionEvents).toBe(0)
    expect(state.series.buckets).toHaveLength(0)
    expect(state.alertsFeed).toHaveLength(0)
    expect(state.dltFeed).toHaveLength(0)
    expect(state.adapters).toHaveLength(0)
    expect(state.phase).toBeNull()
  })

  test('frames applied after a reset keep flowing from the fresh baseline', () => {
    const s = fresh()
    s.applySnapshot({ seq: 9000, totals: { duplicates: 50, alerts: 2, dlt: 1 } })
    s.reset({ seq: 9000, totals: { duplicates: 0, alerts: 0, dlt: 0 } })
    s.apply({ kind: 'duplicate', seq: 9001, eventId: 'e1', adapterId: 'gw-1' })
    s.flush()
    const state = s.getState()
    expect(state.seq).toBe(9001)
    expect(state.series.buckets.at(-1)?.count).toBe(1)
  })
})
