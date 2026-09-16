import { useRef, useState } from 'react'
import { runFullDemo } from '../demo/runFullDemo'
import { simulate } from '../api/gateway'
import { fetchSnapshot, resetDemo } from '../api/hub'
import { store } from '../stream/store'
import { Panel } from './Panel'

const PROFILES = ['low', 'moderate', 'high', 'overload'] as const

export function DemoControl() {
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const abort = useRef(false)

  async function run(profile: string): Promise<void> {
    setBusy(true)
    setError(null)
    try {
      await simulate(profile)
    } catch (cause) {
      setError(String(cause))
    } finally {
      setBusy(false)
    }
  }

  async function runFull(): Promise<void> {
    setBusy(true)
    abort.current = false
    setError(null)
    try {
      const result = await runFullDemo({
        simulate: profile => run(profile).then(() => undefined),
        snapshot: fetchSnapshot,
        onPhase: (name, index, total) => store.setPhase({ name, index, total }),
        shouldStop: () => abort.current,
      })
      if (result === 'aborted') setError('demo aborted')
    } catch (cause) {
      setError(String(cause))
    } finally {
      store.setPhase(null)
      setBusy(false)
    }
  }

  async function reset(): Promise<void> {
    setBusy(true)
    setError(null)
    try {
      store.reset(await resetDemo())
    } catch (cause) {
      setError(String(cause))
    } finally {
      setBusy(false)
    }
  }

  return (
    <Panel label="demo control">
      <div className="flex items-center gap-2 text-[10px]">
        {PROFILES.map(profile => (
          <button
            key={profile}
            type="button"
            disabled={busy}
            onClick={() => void run(profile)}
            className="rounded border border-line px-3 py-1.5 uppercase tracking-widest text-muted transition hover:border-up hover:text-up disabled:opacity-40"
          >
            {profile}
          </button>
        ))}
        <button
          type="button"
          disabled={busy}
          onClick={() => void runFull()}
          className="rounded border border-up px-4 py-1.5 uppercase tracking-widest text-up transition hover:bg-up/10 disabled:opacity-40"
        >
          run full demo
        </button>
        <button
          type="button"
          disabled={busy}
          onClick={() => void reset()}
          className="rounded border border-down px-4 py-1.5 uppercase tracking-widest text-down transition hover:bg-down/10 disabled:opacity-40"
        >
          reset demo
        </button>
        {busy && (
          <button type="button" onClick={() => { abort.current = true }} className="text-down uppercase tracking-widest">
            stop
          </button>
        )}
        {error && <span className="text-down">{error}</span>}
      </div>
    </Panel>
  )
}
