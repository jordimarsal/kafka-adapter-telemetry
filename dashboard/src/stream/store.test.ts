import { beforeEach, describe, expect, test, vi } from 'vitest'
import { store } from './store'
import type { StreamFrame } from './types'

const telemetry = (seq: number, latencyMs = 100): StreamFrame =>
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
