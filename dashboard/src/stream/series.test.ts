import { describe, expect, test } from 'vitest'
import { emptySeries, foldTelemetry, histogram, latenciesWithin, markReset, percentile, rateAt } from './series'

describe('foldTelemetry', () => {
  test('counts events into one-second buckets', () => {
    let s = emptySeries()
    s = foldTelemetry(s, 100, 1000)
    s = foldTelemetry(s, 200, 1500)
    s = foldTelemetry(s, 300, 2500)
    expect(rateAt(s, 1)).toBe(2)
    expect(rateAt(s, 2)).toBe(1)
  })

  test('keeps at most 120 buckets', () => {
    let s = emptySeries()
    for (let i = 0; i < 150; i++) {
      s = foldTelemetry(s, 10, i * 1000)
    }
    expect(s.buckets).toHaveLength(120)
    expect(s.buckets[0].second).toBe(30)
  })

  test('freezes samples at the cap while count keeps counting', () => {
    let s = emptySeries()
    for (let i = 0; i < 70; i++) {
      s = foldTelemetry(s, 100 + i, 1000)
    }
    expect(s.buckets[0].count).toBe(70)
    expect(s.buckets[0].samples).toHaveLength(64)
  })
})

describe('percentile', () => {
  test('returns 0 on empty samples', () => {
    expect(percentile([], 0.95)).toBe(0)
  })

  test('returns the nearest-rank value', () => {
    const samples = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    expect(percentile(samples, 0.5)).toBe(50)
    expect(percentile(samples, 0.95)).toBe(100)
  })
})

describe('histogram', () => {
  test('bins latencies in 100ms bins, clamping the last bin', () => {
    let s = emptySeries()
    s = foldTelemetry(s, 50, 1000)
    s = foldTelemetry(s, 150, 1000)
    s = foldTelemetry(s, 9999, 1000)
    const bins = histogram(s, 0)
    expect(bins[0]).toBe(1)
    expect(bins[1]).toBe(1)
    expect(bins[59]).toBe(1)
    expect(bins).toHaveLength(60)
  })
})

describe('latenciesWithin', () => {
  test('collects samples from the given second on', () => {
    let s = emptySeries()
    s = foldTelemetry(s, 10, 0)
    s = foldTelemetry(s, 20, 1000)
    expect(latenciesWithin(s, 1)).toEqual([20])
  })
})

test('markReset records the second of the gap', () => {
  const s = markReset(foldTelemetry(emptySeries(), 10, 5000), 9000)
  expect(s.resets).toEqual([9])
})
