import { useDashboard } from '../stream/store'
import { Panel } from './Panel'

const FLOW: { label: string; hint: string }[] = [
  { label: 'ADAPTERS', hint: 'POST /telemetry' },
  { label: 'KAFKA', hint: 'adapter.telemetry.v1' },
  { label: 'HUB', hint: 'idempotent upsert' },
  { label: 'ORACLE', hint: 'TELEMETRY_EVENT' },
]

function nodeValue(index: number, session: number, seq: number, alerts: number): string {
  if (index === 0) return `${session} ev`
  if (index === 2) return `seq ${seq}`
  if (index === 3) return `${alerts} alerts`
  return ''
}

export function PipelineFlow() {
  const session = useDashboard(state => state.sessionEvents)
  const seq = useDashboard(state => state.seq)
  const alerts = useDashboard(state => state.totals.alerts)
  return (
    <Panel label="pipeline">
      <div className="flex h-full flex-col justify-center gap-3 text-[10px]">
        {FLOW.map((node, index) => (
          <div key={node.label} className="flex items-center gap-3">
            <span className="w-20 shrink-0 text-dim">{node.label}</span>
            <span className="w-28 shrink-0 text-muted">{node.hint}</span>
            <svg viewBox="0 0 120 8" className="h-2 flex-1" preserveAspectRatio="none" aria-hidden>
              <line x1="0" y1="4" x2="120" y2="4" stroke="#1c2530" strokeWidth="2" />
              <line x1="0" y1="4" x2="120" y2="4" stroke="#3ddc97" strokeWidth="2" strokeDasharray="6 14" className="flow-line" />
            </svg>
            <span className="w-20 shrink-0 text-right text-fg">{nodeValue(index, session, seq, alerts)}</span>
          </div>
        ))}
      </div>
    </Panel>
  )
}
