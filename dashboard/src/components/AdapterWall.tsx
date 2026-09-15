import { useDashboard } from '../stream/store'
import { Panel } from './Panel'

export function AdapterWall() {
  const adapters = useDashboard(state => state.adapters)
  return (
    <Panel label={`adapters · ${adapters.length}`}>
      <div className="grid grid-cols-3 gap-2 md:grid-cols-6">
        {adapters.map(adapter => {
          const down = adapter.consecutiveDown >= 3 || adapter.alertActive
          const degraded = !down && adapter.consecutiveDown > 0
          return (
            <div key={adapter.adapterId} className={`rounded border px-2 py-1.5 text-[10px] ${down ? 'border-down' : degraded ? 'border-warn' : 'border-line'}`}>
              <div className="truncate text-fg">{adapter.adapterId}</div>
              <div className={down ? 'text-down' : degraded ? 'text-warn' : 'text-up'}>
                {down ? 'DOWN' : degraded ? 'DEGRADED' : 'UP'} · {adapter.consecutiveDown}↓
              </div>
              {adapter.alertActive && <div className="text-down">alert active</div>}
            </div>
          )
        })}
      </div>
    </Panel>
  )
}
