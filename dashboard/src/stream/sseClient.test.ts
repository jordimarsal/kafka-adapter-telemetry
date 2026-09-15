import { expect, test } from 'vitest'
import { connectStream, type EventSourceLike } from './sseClient'

class FakeEventSource implements EventSourceLike {
  url: string
  onopen: (() => void) | null = null
  onerror: (() => void) | null = null
  closed = false
  private listeners = new Map<string, (event: { data: string }) => void>()

  constructor(url: string) {
    this.url = url
  }

  addEventListener(name: string, listener: (event: { data: string }) => void) {
    this.listeners.set(name, listener)
  }

  emit(name: string, data: object) {
    this.listeners.get(name)?.({ data: JSON.stringify(data) })
  }

  close() {
    this.closed = true
  }
}

test('connects to the hub stream and stamps frames with their event kind', () => {
  const frames: unknown[] = []
  let source: FakeEventSource | undefined
  const close = connectStream(
    { onFrame: f => frames.push(f), onStatus: () => {}, onGap: () => {} },
    url => (source = new FakeEventSource(url)),
  )
  expect(source?.url).toBe('/api/v1/stream')
  source!.emit('telemetry', { seq: 1, eventId: 'e1', adapterId: 'gw-1', status: 'UP', latencyMs: 10, country: 'ES', occurredAt: '2026-09-15T10:00:00Z' })
  expect(frames[0]).toMatchObject({ kind: 'telemetry', seq: 1, adapterId: 'gw-1' })
  close()
  expect(source!.closed).toBe(true)
})

test('reports live and reconnecting status', () => {
  const statuses: string[] = []
  let source: FakeEventSource | undefined
  const close = connectStream(
    { onFrame: () => {}, onStatus: s => statuses.push(s), onGap: () => {} },
    url => (source = new FakeEventSource(url)),
  )
  source!.onopen!()
  source!.onerror!()
  expect(statuses).toEqual(['live', 'reconnecting'])
  close()
})

test('detects seq gaps and calls onGap once per gap', () => {
  const gaps: number[] = []
  let source: FakeEventSource | undefined
  const close = connectStream(
    { onFrame: () => {}, onStatus: () => {}, onGap: () => gaps.push(1) },
    url => (source = new FakeEventSource(url)),
  )
  source!.emit('telemetry', { seq: 1 })
  source!.emit('telemetry', { seq: 5 })
  source!.emit('telemetry', { seq: 6 })
  source!.emit('telemetry', { seq: 3 })
  expect(gaps).toHaveLength(1)
  close()
})
