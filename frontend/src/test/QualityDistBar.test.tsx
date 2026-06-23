/**
 * QualityDistBar unit tests.
 *
 * Spec ref: SPEC-15 §7 Test Plan — renders distribution; accessible label present;
 * handles empty/zero distribution gracefully.
 */
import { describe, it, expect, vi, beforeAll } from 'vitest'
import { render, screen } from '@testing-library/react'
import { QualityDistBar } from '../components/stream/QualityDistBar'

// Polyfill ResizeObserver used by Recharts inside jsdom.
beforeAll(() => {
  class MockResizeObserver {
    observe = vi.fn()
    unobserve = vi.fn()
    disconnect = vi.fn()
  }
  globalThis.ResizeObserver = MockResizeObserver as unknown as typeof ResizeObserver
})

const SAMPLE_DIST: Record<string, number> = {
  '1080p': 45.2,
  '720p': 24.1,
  '480p': 18.6,
  '360p': 8.3,
  '144p': 3.8,
}

describe('QualityDistBar', () => {
  it('has accessible aria-label', () => {
    render(<QualityDistBar distribution={SAMPLE_DIST} />)
    expect(screen.getByRole('img', { name: 'Quality distribution bar' })).toBeDefined()
  })

  it('renders legend entries for all 5 quality tiers', () => {
    render(<QualityDistBar distribution={SAMPLE_DIST} />)
    // Each tier has a legend label showing tier name + percentage.
    const legends = screen.getAllByText(/1080p|720p|480p|360p|144p/)
    // There are at least 5 legend entries (could also be in LabelList).
    expect(legends.length).toBeGreaterThanOrEqual(5)
  })

  it('renders 1080p percentage in the legend', () => {
    render(<QualityDistBar distribution={SAMPLE_DIST} />)
    // Legend: "1080p 45.2%"
    const el = screen.getByText(/1080p 45\.2%/)
    expect(el).toBeDefined()
  })

  it('renders 144p percentage in the legend', () => {
    render(<QualityDistBar distribution={SAMPLE_DIST} />)
    const el = screen.getByText(/144p 3\.8%/)
    expect(el).toBeDefined()
  })

  it('handles empty distribution (all zeros) without crashing', () => {
    render(<QualityDistBar distribution={{}} />)
    // Should render the container and legend with 0.0% for all tiers.
    const el = screen.getByRole('img', { name: 'Quality distribution bar' })
    expect(el).toBeDefined()
    // Legend still shows 0.0% for each tier.
    const zeroLabels = screen.getAllByText(/0\.0%/)
    expect(zeroLabels.length).toBe(5) // one per tier
  })

  it('handles partial distribution (missing tiers default to 0)', () => {
    render(<QualityDistBar distribution={{ '1080p': 60, '720p': 40 }} />)
    // Missing tiers show 0.0%.
    const el480 = screen.getByText(/480p 0\.0%/)
    expect(el480).toBeDefined()
  })
})
