import { fetchAdapters, fetchSnapshot } from '../api/hub'
import { connectStream } from '../stream/sseClient'
import { store } from '../stream/store'

export function startDashboard(): () => void {
  store.setStatus('connecting')
  void refreshSnapshot()
  void refreshAdapters()
  const poll = window.setInterval(() => void refreshAdapters(), 3000)
  const close = connectStream({
    onFrame: frame => store.apply(frame),
    onStatus: status => store.setStatus(status),
    // store.apply already marks the series reset on a seq gap; here we only resync totals
    onGap: () => {
      void refreshSnapshot()
    },
  })
  return () => {
    window.clearInterval(poll)
    close()
  }
}

async function refreshSnapshot(): Promise<void> {
  try {
    store.applySnapshot(await fetchSnapshot())
  } catch {
    store.setStatus('reconnecting')
  }
}

async function refreshAdapters(): Promise<void> {
  try {
    store.setAdapters(await fetchAdapters())
  } catch {
    // keep the last known wall; the next poll retries
  }
}
