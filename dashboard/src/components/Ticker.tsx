import { useDashboard } from '../stream/store'

const format = (n: number) => n.toLocaleString('en-US')

export function Ticker() {
  const totals = useDashboard(state => state.totals)
  const session = useDashboard(state => state.sessionEvents)
  const phase = useDashboard(state => state.phase)
  const status = useDashboard(state => state.status)
  return (
    <div className="flex items-center gap-6 rounded-lg border border-line bg-panel px-4 py-2 text-xs">
      <span className={status === 'live' ? 'text-up' : 'text-warn'}>
        ● {status.toUpperCase()}
      </span>
      <span><span className="text-dim">events</span> {format(session)}</span>
      <span><span className="text-dim">duplicates</span> {format(totals.duplicates)}</span>
      <span><span className="text-dim">alerts</span> {format(totals.alerts)}</span>
      <span><span className="text-dim">dlt</span> {format(totals.dlt)}</span>
      {phase && (
        <span className="ml-auto text-up">
          PHASE {phase.index}/{phase.total} — {phase.name.toUpperCase()}
        </span>
      )}
    </div>
  )
}
