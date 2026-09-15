import { render, screen } from '@testing-library/react'
import { expect, test } from 'vitest'
import { store } from '../stream/store'
import { AlertsFeed } from './AlertsFeed'

test('renders the newest alert first', () => {
  store.apply({ kind: 'alert', seq: 1, alertId: 'a1', adapterId: 'gw-1', reason: '3 DOWN', raisedAt: 'x', triggerEventId: 'e1' })
  store.apply({ kind: 'alert', seq: 2, alertId: 'a2', adapterId: 'gw-2', reason: '3 DOWN', raisedAt: 'x', triggerEventId: 'e2' })
  store.flush()
  render(<AlertsFeed />)
  const items = screen.getAllByText(/gw-/)
  expect(items[0]).toHaveTextContent('gw-2')
})
