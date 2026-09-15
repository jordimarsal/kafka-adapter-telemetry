import { cleanup, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, expect, test, vi } from 'vitest'
import { DemoControl } from './DemoControl'

afterEach(cleanup)

const simulate = vi.fn(async (_profile: string) => {})
const runFullDemo = vi.fn(async (_deps?: unknown): Promise<'completed' | 'aborted'> => 'completed')

vi.mock('../api/gateway', () => ({ simulate: (profile: string) => simulate(profile) }))
vi.mock('../demo/runFullDemo', () => ({ runFullDemo: (deps: unknown) => runFullDemo(deps) }))

test('profile buttons fire the gateway simulate call', async () => {
  render(<DemoControl />)
  await userEvent.click(screen.getByRole('button', { name: 'overload' }))
  expect(simulate).toHaveBeenCalledWith('overload')
})

test('a mid-demo failure surfaces the error and re-enables the controls', async () => {
  const failure = new Error('hub unreachable')
  runFullDemo.mockRejectedValueOnce(failure)
  render(<DemoControl />)
  await userEvent.click(screen.getByRole('button', { name: 'run full demo' }))
  await waitFor(() => expect(screen.getByText(/hub unreachable/)).toBeInTheDocument())
  expect(screen.getByRole('button', { name: 'run full demo' })).toBeEnabled()
})
