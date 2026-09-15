export interface DemoDeps {
  simulate: (profile: string) => Promise<void>
  snapshot: () => Promise<{ seq: number }>
  onPhase?: (name: string, index: number, total: number) => void
  shouldStop?: () => boolean
  sleep?: (ms: number) => Promise<void>
}

export const PHASES: readonly string[] = ['low', 'high', 'overload', 'overload']
const STABLE_READS = 3
const POLL_MS = 2000

export async function runFullDemo(deps: DemoDeps): Promise<'completed' | 'aborted'> {
  const sleep = deps.sleep ?? (ms => new Promise(resolve => setTimeout(resolve, ms)))
  for (const [index, name] of PHASES.entries()) {
    if (deps.shouldStop?.()) return 'aborted'
    deps.onPhase?.(name, index + 1, PHASES.length)
    await deps.simulate(name)
    await waitForStable(deps, sleep)
  }
  return 'completed'
}

async function waitForStable(deps: DemoDeps, sleep: (ms: number) => Promise<void>): Promise<void> {
  let previous = -1
  let stable = 0
  while (stable < STABLE_READS) {
    await sleep(POLL_MS)
    if (deps.shouldStop?.()) return
    const current = (await deps.snapshot()).seq
    if (current === previous) {
      stable += 1
    } else {
      stable = 0
      previous = current
    }
  }
}
