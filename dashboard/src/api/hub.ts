import type { AdapterSummary, Snapshot } from '../stream/types'

async function getJson<T>(url: string): Promise<T> {
  const response = await fetch(url)
  if (!response.ok) throw new Error(`GET ${url} -> ${response.status}`)
  return response.json() as Promise<T>
}

export const fetchSnapshot = (): Promise<Snapshot> => getJson<Snapshot>('/api/v1/metrics/snapshot')
export const fetchAdapters = (): Promise<AdapterSummary[]> => getJson<AdapterSummary[]>('/api/v1/adapters')
