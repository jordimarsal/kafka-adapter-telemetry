import { render, screen } from '@testing-library/react'
import { expect, test } from 'vitest'
import { store } from '../stream/store'
import { IntegrityPanel } from './IntegrityPanel'

test('integrity shows duplicate and dlt totals with recent dlt reasons', () => {
  store.applySnapshot({ seq: 9, totals: { duplicates: 95, alerts: 14, dlt: 20 } })
  store.apply({ kind: 'dlt', seq: 10, reason: 'payload too large' })
  store.flush()
  render(<IntegrityPanel />)
  expect(screen.getByText('95')).toBeInTheDocument()
  expect(screen.getByText('20')).toBeInTheDocument()
  expect(screen.getByText('duplicates rejected')).toBeInTheDocument()
  expect(screen.getByText('dead letters')).toBeInTheDocument()
  expect(screen.getByText(/dlt · payload too large/)).toBeInTheDocument()
})
