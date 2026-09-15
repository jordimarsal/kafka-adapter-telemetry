import { render, screen } from '@testing-library/react'
import { expect, test } from 'vitest'
import { store } from '../stream/store'
import { PipelineFlow } from './PipelineFlow'

test('pipeline shows the session event count and the hub seq', () => {
  store.apply({ kind: 'telemetry', seq: 3, eventId: 'e', adapterId: 'gw-1', status: 'UP', latencyMs: 10, country: 'ES', occurredAt: 'x' })
  store.flush()
  render(<PipelineFlow />)
  expect(screen.getByText('pipeline')).toBeInTheDocument()
  expect(screen.getByText('1 ev')).toBeInTheDocument()
  expect(screen.getByText('seq 3')).toBeInTheDocument()
})
