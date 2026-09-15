import { describe, expect, test } from 'vitest'
import { emptySeries, foldTelemetry, markReset } from '../stream/series'
import { latencyOption, throughputOption } from './options'

describe('throughputOption', () => {
  test('fills missing seconds with zero', () => {
    let series = emptySeries()
    series = foldTelemetry(series, 10, 0)
    series = foldTelemetry(series, 10, 2000)
    const option = throughputOption(series)
    const data = (option.series as { data: number[] }[])[0].data
    expect(data).toEqual([1, 0, 1])
  })

  test('adds a reset markLine per recorded reset', () => {
    let series = emptySeries()
    series = foldTelemetry(series, 10, 0)
    series = markReset(series, 5000)
    const option = throughputOption(series)
    const marks = (option.series as { markLine: { data: unknown[] } }[])[0].markLine.data
    expect(marks).toHaveLength(1)
  })
})

describe('latencyOption', () => {
  test('returns 60 bins and marks p50/p95', () => {
    let series = emptySeries()
    for (let i = 0; i < 10; i++) {
      series = foldTelemetry(series, i * 10, i * 1000)
    }
    const option = latencyOption(series, 60, 60)
    const first = (option.series as { data: unknown; markLine: { data: { yAxis: number }[] } }[])[0]
    expect(first.data).toHaveLength(60)
    const marks = first.markLine.data
    expect(marks).toHaveLength(2)
    expect(marks[0].yAxis).toBeGreaterThan(0)
  })
})
