const GATEWAY = import.meta.env.VITE_GATEWAY_URL ?? 'http://localhost:8081'

export async function simulate(profile: string): Promise<void> {
  const response = await fetch(`${GATEWAY}/api/v1/telemetry/simulate?profile=${profile}`, { method: 'POST' })
  if (!response.ok) throw new Error(`simulate(${profile}) -> ${response.status}`)
}
