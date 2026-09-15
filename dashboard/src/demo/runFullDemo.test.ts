import { describe, expect, test } from 'vitest'
import { runFullDemo } from './runFullDemo'

const sleep = () => Promise.resolve()
const stableSnapshot = (seq: number) => async () => ({ seq })

describe('runFullDemo', () => {
  test('runs low, high, overload, overload in order and completes', async () => {
    const ran: string[] = []
    const phases: string[] = []
    let seq = 0
    const result = await runFullDemo({
      simulate: async profile => {
        ran.push(profile)
        seq += 10
      },
      snapshot: async () => ({ seq }),
      onPhase: name => phases.push(name),
      sleep,
    })
    expect(ran).toEqual(['low', 'high', 'overload', 'overload'])
    expect(phases).toEqual(['low', 'high', 'overload', 'overload'])
    expect(result).toBe('completed')
  })

  test('aborts between phases when shouldStop flips', async () => {
    const ran: string[] = []
    let stopped = false
    const result = await runFullDemo({
      simulate: async profile => {
        ran.push(profile)
      },
      snapshot: stableSnapshot(1),
      shouldStop: () => stopped,
      sleep: () => {
        stopped = true
        return Promise.resolve()
      },
    })
    expect(result).toBe('aborted')
    expect(ran).toEqual(['low'])
  })

  test('waits for a stable seq before the next phase', async () => {
    const snapshots: number[] = []
    let seq = 0
    await runFullDemo({
      simulate: async () => {
        seq += 5
      },
      snapshot: async () => {
        snapshots.push(seq)
        return { seq }
      },
      sleep,
    })
    // last three reads before each next phase saw the same seq
    expect(snapshots.slice(-3)).toEqual([seq, seq, seq])
  })
})
