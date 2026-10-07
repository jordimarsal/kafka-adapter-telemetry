import { useDashboard } from '../stream/store'
import type { AdapterSummary } from '../stream/types'
import { Panel } from './Panel'

type Tone = 'down' | 'warn' | 'up'

const BORDER: Record<Tone, string> = { down: 'border-down', warn: 'border-warn', up: 'border-line' }
const TEXT: Record<Tone, string> = { down: 'text-down', warn: 'text-warn', up: 'text-up' }
const LABEL: Record<Tone, string> = { down: 'DOWN', warn: 'DEGRADED', up: 'UP' }

function toneFor({ consecutiveDown, alertActive }: AdapterSummary): Tone {
  if (alertActive || consecutiveDown >= 3) return 'down'
  if (consecutiveDown > 0) return 'warn'
  return 'up'
}

export function AdapterWall() {
  const adapters = useDashboard(state => state.adapters)
  return (
    <Panel label={`adapters · ${adapters.length}`}>
      <div className="grid grid-cols-3 gap-2 md:grid-cols-6">
        {adapters.map(adapter => {
          const tone = toneFor(adapter)
          return (
            <div key={adapter.adapterId} className={`rounded border px-2 py-1.5 text-[10px] ${BORDER[tone]}`}>
              <div className="truncate text-fg">{adapter.adapterId}</div>
              <div className={TEXT[tone]}>
                {LABEL[tone]} · {adapter.consecutiveDown}↓
              </div>
              {adapter.alertActive && <div className="text-down">alert active</div>}
            </div>
          )
        })}
      </div>
    </Panel>
  )
}
