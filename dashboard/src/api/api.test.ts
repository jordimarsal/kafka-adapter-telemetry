import { afterEach, describe, expect, test, vi } from 'vitest'
import { fetchAdapters, fetchSnapshot, resetDemo } from './hub'
import { simulate } from './gateway'

afterEach(() => vi.unstubAllGlobals())

describe('hub client', () => {
  test('fetchSnapshot parses the snapshot payload', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response(JSON.stringify({ seq: 42, totals: { duplicates: 1, alerts: 2, dlt: 3 } }), { status: 200 })))
    const snapshot = await fetchSnapshot()
    expect(snapshot.seq).toBe(42)
    expect(snapshot.totals.dlt).toBe(3)
  })

  test('fetchSnapshot throws on non-2xx', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response('nope', { status: 503 })))
    await expect(fetchSnapshot()).rejects.toThrow('503')
  })

  test('fetchAdapters returns the list', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response(JSON.stringify([{ adapterId: 'gw-1', consecutiveDown: 0, alertActive: false, lastSeen: '2026-09-15T10:00:00Z' }]), { status: 200 })))
    const adapters = await fetchAdapters()
    expect(adapters[0].adapterId).toBe('gw-1')
  })

  test('resetDemo POSTs to the hub and parses the fresh snapshot', async () => {
    const fetchMock = vi.fn(async () => new Response(JSON.stringify({ seq: 9081, totals: { duplicates: 0, alerts: 0, dlt: 0 } }), { status: 200 }))
    vi.stubGlobal('fetch', fetchMock)
    const snapshot = await resetDemo()
    expect(fetchMock).toHaveBeenCalledWith('/api/v1/demo/reset', { method: 'POST' })
    expect(snapshot.seq).toBe(9081)
    expect(snapshot.totals.duplicates).toBe(0)
  })
})

describe('gateway client', () => {
  test('simulate POSTs the profile to the gateway', async () => {
    const fetchMock = vi.fn(async () => new Response('{"accepted":20}', { status: 200 }))
    vi.stubGlobal('fetch', fetchMock)
    await simulate('low')
    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringMatching(/\/api\/v1\/telemetry\/simulate\?profile=low$/),
      { method: 'POST' },
    )
  })

  test('simulate throws on failure', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response('bad profile', { status: 400 })))
    await expect(simulate('nope')).rejects.toThrow('400')
  })
})
