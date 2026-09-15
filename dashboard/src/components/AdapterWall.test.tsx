import { render, screen } from '@testing-library/react'
import { expect, test } from 'vitest'
import { store } from '../stream/store'
import { AdapterWall } from './AdapterWall'

test('wall lists every adapter from the read api', () => {
  store.setAdapters([
    { adapterId: 'gw-es-1', consecutiveDown: 0, alertActive: false, lastSeen: 'x' },
    { adapterId: 'gw-de-2', consecutiveDown: 3, alertActive: true, lastSeen: 'x' },
  ])
  store.flush()
  render(<AdapterWall />)
  expect(screen.getByText('gw-es-1')).toBeInTheDocument()
  expect(screen.getByText('gw-de-2')).toBeInTheDocument()
  expect(screen.getByText(/alert active/)).toBeInTheDocument()
})
