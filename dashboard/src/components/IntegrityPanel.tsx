import { useDashboard } from '../stream/store'
import { Panel } from './Panel'

export function IntegrityPanel() {
  const totals = useDashboard(state => state.totals)
  const dltFeed = useDashboard(state => state.dltFeed)
  return (
    <Panel label="integrity">
      <div className="flex gap-6">
        <div>
          <div className="num">{totals.duplicates}</div>
          <div className="cap">duplicates rejected</div>
        </div>
        <div>
          <div className="num">{totals.dlt}</div>
          <div className="cap">dead letters</div>
        </div>
      </div>
      <ul className="mt-2 space-y-0.5 text-[9px] text-muted">
        {dltFeed.slice(0, 3).map(item => (
          <li key={item.seq} className="truncate">dlt · {item.reason}</li>
        ))}
      </ul>
    </Panel>
  )
}
