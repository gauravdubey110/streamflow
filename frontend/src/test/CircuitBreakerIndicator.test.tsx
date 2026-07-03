/**
 * CircuitBreakerIndicator component tests.
 *
 * Spec ref: SPEC-16 R4 — pill with 3 states, tooltip with reason.
 */
import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { CircuitBreakerIndicator } from '../components/common/CircuitBreakerIndicator'

describe('CircuitBreakerIndicator', () => {
  it('renders CLOSED state with green styling indicator', () => {
    render(<CircuitBreakerIndicator state="CLOSED" />)
    const el = screen.getByTestId('cb-indicator')
    expect(el.getAttribute('data-state')).toBe('CLOSED')
    expect(el.textContent).toContain('CB: CLOSED')
  })

  it('renders OPEN state', () => {
    render(<CircuitBreakerIndicator state="OPEN" />)
    const el = screen.getByTestId('cb-indicator')
    expect(el.getAttribute('data-state')).toBe('OPEN')
    expect(el.textContent).toContain('CB: OPEN')
  })

  it('renders HALF_OPEN state', () => {
    render(<CircuitBreakerIndicator state="HALF_OPEN" />)
    const el = screen.getByTestId('cb-indicator')
    expect(el.getAttribute('data-state')).toBe('HALF_OPEN')
    expect(el.textContent).toContain('CB: HALF OPEN')
  })

  it('sets title to reason when provided', () => {
    render(<CircuitBreakerIndicator state="OPEN" reason="Failure rate 60%" />)
    const el = screen.getByTestId('cb-indicator')
    expect(el.getAttribute('title')).toBe('Failure rate 60%')
  })

  it('sets title to state label when reason is not provided', () => {
    render(<CircuitBreakerIndicator state="CLOSED" />)
    const el = screen.getByTestId('cb-indicator')
    expect(el.getAttribute('title')).toBe('CB: CLOSED')
  })

  it('has an accessible aria-label including state', () => {
    render(<CircuitBreakerIndicator state="OPEN" reason="Threshold exceeded" />)
    const el = screen.getByLabelText(/Circuit breaker: OPEN/)
    expect(el).toBeDefined()
  })
})
