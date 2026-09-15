import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, test, vi } from 'vitest'
import { DemoControl } from './DemoControl'

const simulate = vi.fn(async (_profile: string) => {})

vi.mock('../api/gateway', () => ({ simulate: (profile: string) => simulate(profile) }))

test('profile buttons fire the gateway simulate call', async () => {
  render(<DemoControl />)
  await userEvent.click(screen.getByRole('button', { name: 'overload' }))
  expect(simulate).toHaveBeenCalledWith('overload')
})
