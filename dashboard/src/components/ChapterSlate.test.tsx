import { render, screen } from '@testing-library/react'
import { expect, test } from 'vitest'
import { store } from '../stream/store'
import { ChapterSlate } from './ChapterSlate'

test('slate announces the current demo phase with its subtitle', () => {
  store.setPhase({ name: 'high', index: 2, total: 4 })
  store.flush()
  render(<ChapterSlate />)
  expect(screen.getByText(/PHASE 2\/4/)).toBeInTheDocument()
  expect(screen.getByText(/50 ev\/s/)).toBeInTheDocument()
})
