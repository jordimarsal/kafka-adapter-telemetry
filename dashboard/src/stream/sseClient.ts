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
  // native EventSource satisfies EventSourceLike at runtime; DOM typings type onopen/onerror with an event parameter, hence the boundary cast
  open: (url: string) => EventSourceLike = url => new EventSource(url) as unknown as EventSourceLike,
): () => void {
  const source = open('/api/v1/stream')
  let lastSeq = 0
  source.onopen = () => handlers.onStatus('live')
  source.onerror = () => handlers.onStatus('reconnecting')
  for (const name of EVENT_NAMES) {
    source.addEventListener(name, event => {
      let parsed: { seq: number }
      try {
        parsed = JSON.parse(event.data) as { seq: number }
      } catch (parseError) {
        // a frame we cannot read must not poison seq tracking; drop it loudly
        console.warn(`dropping malformed ${name} frame`, parseError)
        return
      }
      if (lastSeq > 0 && parsed.seq > lastSeq + 1) handlers.onGap()
      if (parsed.seq > lastSeq) lastSeq = parsed.seq
      handlers.onFrame({ kind: name, ...parsed } as StreamFrame)
    })
  }
  return () => source.close()
}
