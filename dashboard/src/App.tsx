import { LatencyChart } from './components/LatencyChart'
import { Panel } from './components/Panel'
import { ThroughputChart } from './components/ThroughputChart'

export default function App() {
  return (
    <div className="min-h-screen bg-bg p-4 font-mono text-fg">
      <div className="grid grid-cols-[1fr_2fr_1fr] grid-rows-[auto_minmax(0,1fr)_auto_auto] gap-3">
        <div className="col-span-3 rounded-lg border border-line bg-panel px-4 py-2 text-xs">
          ADAPTER TELEMETRY
        </div>
        <Panel label="pipeline">—</Panel>
        <div className="grid min-h-0 grid-rows-2 gap-3">
          <ThroughputChart />
          <LatencyChart />
        </div>
        <div className="grid min-h-0 grid-rows-2 gap-3">
          <Panel label="alerts · 0">—</Panel>
          <Panel label="integrity">—</Panel>
        </div>
        <div className="col-span-3">
          <Panel label="adapters · 0">—</Panel>
        </div>
        <div className="col-span-3">
          <Panel label="demo control">—</Panel>
        </div>
      </div>
    </div>
  )
}
