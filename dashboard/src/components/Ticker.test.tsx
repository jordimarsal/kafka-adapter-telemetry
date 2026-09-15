import { render, screen } from '@testing-library/react'
import { expect, test } from 'vitest'
import { store } from '../stream/store'
import { Ticker } from './Ticker'

test('ticker shows hub totals and the live session count', () => {
  store.applySnapshot({ seq: 9, totals: { duplicates: 95, alerts: 14, dlt: 20 } })
  store.flush()
  render(<Ticker />)
  expect(screen.getByText('95')).toBeInTheDocument()
  expect(screen.getByText('14')).toBeInTheDocument()
  expect(screen.getByText('20')).toBeInTheDocument()
})
