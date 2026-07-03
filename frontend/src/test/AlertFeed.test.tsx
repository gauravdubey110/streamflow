/**
 * AlertFeed component tests.
 *
 * Spec ref: SPEC-16 R2, NFR1 — renders alerts newest-first, badge colors,
 * empty state, display cap at 100 items.
 */
import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { AlertFeed } from '../components/alerts/AlertFeed'
import type { AlertEvent } from '../types/alert.types'

function makeAlert(overrides?: Partial<AlertEvent>): AlertEvent {
  return {
    alertId: `alert-${Math.random().toString(36).slice(2)}`,
    streamId: 'stream-001',
    severity: 'CRITICAL',
    alertType: 'HIGH_BUFFER_RATE',
    threshold: 5.0,
    actualValue: 8.3,
    message: 'Buffer rate 8.3% exceeds threshold',
    timestamp: Date.now(),
    ...overrides,
  }
}

describe('AlertFeed', () => {
  it('renders "No alerts" when list is empty', () => {
    render(<AlertFeed alerts={[]} />)
    expect(screen.getByTestId('alert-feed-empty')).toBeDefined()
    expect(screen.getByText('No alerts')).toBeDefined()
  })

  it('renders alert rows for each alert', () => {
    const alerts = [makeAlert({ alertId: 'a1' }), makeAlert({ alertId: 'a2' })]
    render(<AlertFeed alerts={alerts} />)
    expect(screen.getByTestId('alert-row-a1')).toBeDefined()
    expect(screen.getByTestId('alert-row-a2')).toBeDefined()
  })

  it('renders CRITICAL badge with red aria-label', () => {
    render(<AlertFeed alerts={[makeAlert({ severity: 'CRITICAL', alertId: 'c1' })]} />)
    expect(screen.getByLabelText('Severity: CRITICAL')).toBeDefined()
  })

  it('renders WARNING badge', () => {
    render(<AlertFeed alerts={[makeAlert({ severity: 'WARNING', alertId: 'w1' })]} />)
    expect(screen.getByLabelText('Severity: WARNING')).toBeDefined()
  })

  it('renders INFO badge', () => {
    render(<AlertFeed alerts={[makeAlert({ severity: 'INFO', alertId: 'i1' })]} />)
    expect(screen.getByLabelText('Severity: INFO')).toBeDefined()
  })

  it('renders the alert message text', () => {
    render(
      <AlertFeed
        alerts={[makeAlert({ message: 'Unique message ABC', alertId: 'msg-1' })]}
      />,
    )
    expect(screen.getByText('Unique message ABC')).toBeDefined()
  })

  it('renders alert type with underscores replaced by spaces', () => {
    render(<AlertFeed alerts={[makeAlert({ alertType: 'HIGH_BUFFER_RATE', alertId: 'type-1' })]} />)
    expect(screen.getByText('HIGH BUFFER RATE')).toBeDefined()
  })

  it('caps displayed items at 100', () => {
    const alerts = Array.from({ length: 120 }, (_, i) =>
      makeAlert({ alertId: `bulk-${i}` }),
    )
    render(<AlertFeed alerts={alerts} />)
    const rows = screen.getAllByRole('listitem')
    expect(rows.length).toBe(100)
  })

  it('renders the alert feed list with aria-label', () => {
    render(<AlertFeed alerts={[makeAlert()]} />)
    expect(screen.getByLabelText('Alert feed')).toBeDefined()
  })
})
