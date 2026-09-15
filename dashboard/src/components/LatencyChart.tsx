import { useEffect, useRef } from 'react'
import * as echarts from 'echarts'
import { store, useDashboard } from '../stream/store'
import { latencyOption } from '../charts/options'
import { Panel } from './Panel'

export function LatencyChart() {
  const holder = useRef<HTMLDivElement>(null)
  const chart = useRef<echarts.ECharts | null>(null)
  const series = useDashboard(state => state.series)

  useEffect(() => {
    if (!holder.current) return
    chart.current = echarts.init(holder.current, null, { renderer: 'svg' })
    const onResize = () => chart.current?.resize()
    window.addEventListener('resize', onResize)
    return () => {
      window.removeEventListener('resize', onResize)
      chart.current?.dispose()
      chart.current = null
    }
  }, [])

  useEffect(() => {
    chart.current?.setOption(latencyOption(store.getState().series, 60))
  }, [series])

  return (
    <Panel label="latency · ms">
      <div ref={holder} className="h-full min-h-24 w-full" />
    </Panel>
  )
}
