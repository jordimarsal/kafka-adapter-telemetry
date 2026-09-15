import type { EChartsOption } from 'echarts'
import { histogram, latenciesWithin, percentile, type Series } from '../stream/series'

const clock = (second: number) => new Date(second * 1000).toLocaleTimeString('en-GB', { hour12: false })
const AXIS = { color: '#4d5a6d', fontSize: 9 } as const
const GRID = { top: 8, right: 8, bottom: 20, left: 36 } as const

export function throughputOption(series: Series): EChartsOption {
  const first = series.buckets[0]?.second ?? 0
  const last = series.buckets.at(-1)?.second ?? 0
  const categories: string[] = []
  const counts: number[] = []
  for (let second = first; second <= last; second++) {
    categories.push(clock(second))
    counts.push(series.buckets.find(bucket => bucket.second === second)?.count ?? 0)
  }
  return {
    animation: false,
    backgroundColor: 'transparent',
    grid: GRID,
    xAxis: { type: 'category', data: categories, axisLabel: AXIS, axisLine: { lineStyle: { color: '#1c2530' } } },
    yAxis: { type: 'value', axisLabel: AXIS, splitLine: { lineStyle: { color: '#121826' } } },
    series: [{
      type: 'bar',
      data: counts,
      itemStyle: { color: '#3ddc97' },
      barWidth: '70%',
      markLine: {
        symbol: 'none',
        label: { show: false },
        lineStyle: { color: '#ff5470' },
        data: series.resets.map(second => ({ xAxis: clock(second) })),
      },
    }],
  }
}

export function latencyOption(series: Series, windowSeconds: number, nowSecond = Math.floor(Date.now() / 1000)): EChartsOption {
  const fromSecond = nowSecond - windowSeconds
  const samples = latenciesWithin(series, fromSecond)
  const bins = histogram(series, fromSecond)
  return {
    animation: false,
    backgroundColor: 'transparent',
    grid: GRID,
    xAxis: {
      type: 'category',
      data: bins.map((_, index) => `${index * 100}`),
      axisLabel: { ...AXIS, interval: 9 },
      axisLine: { lineStyle: { color: '#1c2530' } },
      name: 'ms',
      nameTextStyle: AXIS,
    },
    yAxis: { type: 'value', axisLabel: AXIS, splitLine: { lineStyle: { color: '#121826' } } },
    series: [{
      type: 'bar',
      data: bins,
      itemStyle: { color: '#ffb347' },
      markLine: {
        symbol: 'none',
        label: { formatter: '{b}: {c}', color: '#8b98ab', fontSize: 9 },
        lineStyle: { color: '#3ddc97', type: 'dashed' },
        data: [
          { yAxis: percentile(samples, 0.5), name: 'p50' },
          { yAxis: percentile(samples, 0.95), name: 'p95' },
        ],
      },
    }],
  }
}
