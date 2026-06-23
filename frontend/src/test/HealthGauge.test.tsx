/**
 * HealthGauge unit tests.
 *
 * Spec ref: SPEC-15 §7 Test Plan — snapshot tests for each color band.
 */
import { describe, it, expect, vi, beforeAll } from 'vitest'
import { render, screen } from '@testing-library/react'
import { HealthGauge } from '../components/stream/HealthGauge'

// Polyfill ResizeObserver used by Recharts inside jsdom.
beforeAll(() => {
  class MockResizeObserver {
    observe = vi.fn()
    unobserve = vi.fn()
    disconnect = vi.fn()
  }
  globalThis.ResizeObserver = MockResizeObserver as unknown as typeof ResizeObserver
})

describe('HealthGauge', () => {
  it('renders the numeric score in the center', () => {
    render(<HealthGauge score={95} />)
    // The score is rendered as text in the center overlay div.
    expect(screen.getByText('95')).toBeDefined()
  })

  it('renders "health" label text', () => {
    render(<HealthGauge score={80} />)
    expect(screen.getByText('health')).toBeDefined()
  })

  it('has an accessible aria-label including the score', () => {
    render(<HealthGauge score={72} />)
    const el = screen.getByRole('img')
    expect(el.getAttribute('aria-label')).toBe('Health score: 72 out of 100')
  })

  it('clamps score above 100 to 100', () => {
    render(<HealthGauge score={150} />)
    expect(screen.getByText('100')).toBeDefined()
    const el = screen.getByRole('img')
    expect(el.getAttribute('aria-label')).toBe('Health score: 100 out of 100')
  })

  it('clamps score below 0 to 0', () => {
    render(<HealthGauge score={-10} />)
    expect(screen.getByText('0')).toBeDefined()
    const el = screen.getByRole('img')
    expect(el.getAttribute('aria-label')).toBe('Health score: 0 out of 100')
  })

  it('applies green text class for score >= 85 (healthy band)', () => {
    const { container } = render(<HealthGauge score={90} />)
    // The center score span should have the green health color class.
    const scoreSpan = container.querySelector('.text-health-good')
    expect(scoreSpan).not.toBeNull()
  })

  it('applies yellow text class for score in 60-84 (warning band)', () => {
    const { container } = render(<HealthGauge score={70} />)
    const scoreSpan = container.querySelector('.text-health-warn')
    expect(scoreSpan).not.toBeNull()
  })

  it('applies red text class for score < 60 (critical band)', () => {
    const { container } = render(<HealthGauge score={45} />)
    const scoreSpan = container.querySelector('.text-health-bad')
    expect(scoreSpan).not.toBeNull()
  })

  it('renders score 85 in green band (boundary)', () => {
    const { container } = render(<HealthGauge score={85} />)
    const scoreSpan = container.querySelector('.text-health-good')
    expect(scoreSpan).not.toBeNull()
  })

  it('renders score 60 in yellow band (boundary)', () => {
    const { container } = render(<HealthGauge score={60} />)
    const scoreSpan = container.querySelector('.text-health-warn')
    expect(scoreSpan).not.toBeNull()
  })

  it('renders score 59 in red band (just below yellow boundary)', () => {
    const { container } = render(<HealthGauge score={59} />)
    const scoreSpan = container.querySelector('.text-health-bad')
    expect(scoreSpan).not.toBeNull()
  })
})
