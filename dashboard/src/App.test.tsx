import { render, screen } from '@testing-library/react'
import { expect, test } from 'vitest'
import App from './App'

test('renders the dashboard shell', () => {
  render(<App />)
  expect(screen.getByText('ADAPTER TELEMETRY')).toBeInTheDocument()
})
