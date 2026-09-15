import { render, screen } from '@testing-library/react'
import { expect, test, vi } from 'vitest'
import App from './App'

vi.mock('./app/lifecycle', () => ({ startDashboard: () => () => {} }))

test('renders the mission control grid regions', () => {
  render(<App />)
  expect(screen.getByText('throughput · ev/s')).toBeInTheDocument()
  expect(screen.getByText('latency · ms')).toBeInTheDocument()
  expect(screen.getByText('pipeline')).toBeInTheDocument()
  expect(screen.getByText(/alerts ·/)).toBeInTheDocument()
  expect(screen.getByText('integrity')).toBeInTheDocument()
  expect(screen.getByText(/adapters ·/)).toBeInTheDocument()
  expect(screen.getByText('demo control')).toBeInTheDocument()
})
