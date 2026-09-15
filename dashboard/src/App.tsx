import { useEffect } from 'react'
import { startDashboard } from './app/lifecycle'
import { LatencyChart } from './components/LatencyChart'
import { AdapterWall } from './components/AdapterWall'
import { AlertsFeed } from './components/AlertsFeed'
import { ChapterSlate } from './components/ChapterSlate'
import { DemoControl } from './components/DemoControl'
import { IntegrityPanel } from './components/IntegrityPanel'
import { PipelineFlow } from './components/PipelineFlow'
import { ThroughputChart } from './components/ThroughputChart'
import { Ticker } from './components/Ticker'

export default function App() {
  useEffect(() => startDashboard(), [])
  return (
    <div className="min-h-screen bg-bg p-4 font-mono text-fg">
      <div className="grid grid-cols-[1fr_2fr_1fr] grid-rows-[auto_minmax(0,1fr)_auto_auto] gap-3">
        <div className="col-span-3"><Ticker /></div>
        <PipelineFlow />
        <div className="grid min-h-0 grid-rows-2 gap-3">
          <ThroughputChart />
          <LatencyChart />
        </div>
        <div className="grid min-h-0 grid-rows-2 gap-3">
          <AlertsFeed />
          <IntegrityPanel />
        </div>
        <div className="col-span-3"><AdapterWall /></div>
        <div className="col-span-3"><DemoControl /></div>
      </div>
      <ChapterSlate />
    </div>
  )
}
