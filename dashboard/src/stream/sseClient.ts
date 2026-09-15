import type { ConnectionStatus, StreamFrame } from './types'

export interface EventSourceLike {
  onopen: (() => void) | null
  onerror: (() => void) | null
  addEventListener(name: string, listener: (event: { data: string }) => void): void
  close(): void
}

export interface StreamHandlers {
  onFrame: (frame: StreamFrame) => void
  onStatus: (status: ConnectionStatus) => void
  onGap: () => void
}

const EVENT_NAMES = ['telemetry', 'duplicate', 'alert', 'dlt', 'heartbeat'] as const

export function connectStream(
  handlers: StreamHandlers,
  open: (url: string) => EventSourceLike = url => new EventSource(url),
): () => void {
  const source = open('/api/v1/stream')
  let lastSeq = 0
  source.onopen = () => handlers.onStatus('live')
  source.onerror = () => handlers.onStatus('reconnecting')
  for (const name of EVENT_NAMES) {
    source.addEventListener(name, event => {
      const parsed = JSON.parse(event.data) as { seq: number }
      if (lastSeq > 0 && parsed.seq > lastSeq + 1) handlers.onGap()
      if (parsed.seq > lastSeq) lastSeq = parsed.seq
      handlers.onFrame({ kind: name, ...parsed } as StreamFrame)
    })
  }
  return () => source.close()
}
