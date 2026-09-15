import { expect, test, vi } from 'vitest'
import { store } from '../stream/store'
import { startDashboard } from './lifecycle'

vi.mock('../stream/sseClient', () => ({
  connectStream: (handlers: { onFrame: (f: unknown) => void }) => {
    handlers.onFrame({ kind: 'telemetry', seq: 1, eventId: 'e', adapterId: 'gw-1', status: 'UP', latencyMs: 10, country: 'ES', occurredAt: 'x' })
    return () => {}
  },
}))

vi.mock('../api/hub', () => ({
  fetchSnapshot: async () => ({ seq: 0, totals: { duplicates: 7, alerts: 0, dlt: 0 } }),
  fetchAdapters: async () => [{ adapterId: 'gw-1', consecutiveDown: 0, alertActive: false, lastSeen: 'x' }],
}))

test('startDashboard loads snapshot and adapters and streams frames', async () => {
  const stop = startDashboard()
  await vi.waitFor(() => {
    expect(store.getState().totals.duplicates).toBe(7)
    expect(store.getState().adapters[0].adapterId).toBe('gw-1')
    expect(store.getState().sessionEvents).toBe(1)
  })
  stop()
})
