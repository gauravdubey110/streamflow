/**
 * BufferRateBadge unit tests.
 *
 * Spec ref: SPEC-15 §7 Test Plan — color bands; pulse on red; numeric value shown.
 */
import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BufferRateBadge } from '../components/stream/BufferRateBadge'

describe('BufferRateBadge', () => {
  it('shows the numeric rate as text', () => {
    render(<BufferRateBadge rate={1.8} />)
    expect(screen.getByText(/1\.8%/)).toBeDefined()
  })

  it('shows "buf" label text', () => {
    render(<BufferRateBadge rate={1.8} />)
    expect(screen.getByText('buf')).toBeDefined()
  })

  it('has accessible aria-label including the rate', () => {
    render(<BufferRateBadge rate={1.8} />)
    const el = screen.getByLabelText(/Buffer rate: 1\.8 percent/)
    expect(el).toBeDefined()
  })

  // ── Color band tests ──

  it('applies green color classes when rate < 2 (healthy)', () => {
    const { container } = render(<BufferRateBadge rate={1.0} />)
    const badge = container.firstChild as HTMLElement
    expect(badge.className).toContain('text-green-400')
    expect(badge.className).toContain('bg-green-950')
  })

  it('applies yellow color classes when rate is 2 (warning boundary)', () => {
    const { container } = render(<BufferRateBadge rate={2.0} />)
    const badge = container.firstChild as HTMLElement
    expect(badge.className).toContain('text-yellow-400')
    expect(badge.className).toContain('bg-yellow-950')
  })

  it('applies yellow color classes when rate is 5 (upper warning boundary)', () => {
    const { container } = render(<BufferRateBadge rate={5.0} />)
    const badge = container.firstChild as HTMLElement
    expect(badge.className).toContain('text-yellow-400')
    expect(badge.className).toContain('bg-yellow-950')
  })

  it('applies red color classes when rate > 5 (critical)', () => {
    const { container } = render(<BufferRateBadge rate={8.3} />)
    const badge = container.firstChild as HTMLElement
    expect(badge.className).toContain('text-red-400')
    expect(badge.className).toContain('bg-red-950')
  })

  // ── Pulse animation tests ──

  it('includes animate-pulse class when rate > 5 (red)', () => {
    const { container } = render(<BufferRateBadge rate={6.0} />)
    const badge = container.firstChild as HTMLElement
    expect(badge.className).toContain('animate-pulse')
  })

  it('does NOT include animate-pulse when rate is green (< 2)', () => {
    const { container } = render(<BufferRateBadge rate={1.5} />)
    const badge = container.firstChild as HTMLElement
    expect(badge.className).not.toContain('animate-pulse')
  })

  it('does NOT include animate-pulse when rate is yellow (2–5)', () => {
    const { container } = render(<BufferRateBadge rate={3.5} />)
    const badge = container.firstChild as HTMLElement
    expect(badge.className).not.toContain('animate-pulse')
  })

  // ── Boundary values ──

  it('renders 0.0% when rate is 0', () => {
    render(<BufferRateBadge rate={0} />)
    expect(screen.getByText(/0\.0%/)).toBeDefined()
  })

  it('applies green for rate exactly 1.9 (just below warning threshold)', () => {
    const { container } = render(<BufferRateBadge rate={1.9} />)
    const badge = container.firstChild as HTMLElement
    expect(badge.className).toContain('text-green-400')
  })

  it('applies red for rate exactly 5.1 (just above yellow threshold)', () => {
    const { container } = render(<BufferRateBadge rate={5.1} />)
    const badge = container.firstChild as HTMLElement
    expect(badge.className).toContain('text-red-400')
  })
})
