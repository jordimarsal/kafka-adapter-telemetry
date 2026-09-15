export const BUCKET_MS = 1000
export const MAX_BUCKETS = 120
export const LAT_BIN_MS = 100
export const LAT_BINS = 60
export const SAMPLE_CAP = 64

export interface Bucket {
  second: number
  count: number
  samples: number[]
}

export interface Series {
  buckets: Bucket[]
  resets: number[]
}

export function emptySeries(): Series {
  return { buckets: [], resets: [] }
}

export function foldTelemetry(series: Series, latencyMs: number, nowMs: number): Series {
  const second = Math.floor(nowMs / BUCKET_MS)
  const last = series.buckets.at(-1)
  const previous = last && last.second === second ? last : undefined
  const samples = previous && previous.samples.length < SAMPLE_CAP ? [...previous.samples, latencyMs] : previous ? previous.samples : [latencyMs]
  const bucket: Bucket = { second, count: (previous?.count ?? 0) + 1, samples }
  const buckets = previous ? [...series.buckets.slice(0, -1), bucket] : [...series.buckets, bucket]
  return { resets: series.resets, buckets: buckets.slice(-MAX_BUCKETS) }
}

export function markReset(series: Series, nowMs: number): Series {
  return { buckets: series.buckets, resets: [...series.resets, Math.floor(nowMs / BUCKET_MS)] }
}

export function rateAt(series: Series, second: number): number {
  return series.buckets.find(bucket => bucket.second === second)?.count ?? 0
}

export function latenciesWithin(series: Series, fromSecond: number): number[] {
  return series.buckets.filter(bucket => bucket.second >= fromSecond).flatMap(bucket => bucket.samples)
}

export function histogram(series: Series, fromSecond: number): number[] {
  const bins = Array.from({ length: LAT_BINS }, () => 0)
  for (const latency of latenciesWithin(series, fromSecond)) {
    const bin = Math.min(Math.floor(latency / LAT_BIN_MS), LAT_BINS - 1)
    bins[bin] += 1
  }
  return bins
}

export function percentile(samples: number[], q: number): number {
  if (samples.length === 0) return 0
  const sorted = [...samples].sort((a, b) => a - b)
  return sorted[Math.max(0, Math.ceil(q * sorted.length) - 1)]
}
