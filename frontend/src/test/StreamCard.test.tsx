/**
 * StreamCard unit tests.
 *
 * Spec ref: SPEC-08 Test Plan — render card with mock data; assert viewer
 * count and delta shown; assert reconnecting overlay when disconnected.
 * SPEC-15 — HealthGauge, QualityDistBar, BufferRateBadge integrated into layout.
 */
import { describe, it, expect, vi, beforeAll, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { StreamCard } from '../components/stream/StreamCard'

// Polyfill ResizeObserver used by Recharts inside jsdom.
// Must be a class (constructable) so `new ResizeObserver(cb)` succeeds.
beforeAll(() => {
  class MockResizeObserver {
    observe = vi.fn()
    unobserve = vi.fn()
    disconnect = vi.fn()
  }
  globalThis.ResizeObserver = MockResizeObserver as unknown as typeof ResizeObserver
})

// Mock useStreamMetrics so StreamCard renders without a live WebSocket.
const mockUseStreamMetrics = vi.fn()

vi.mock('../hooks/useStreamMetrics', () => ({
  useStreamMetrics: (streamId: string) => mockUseStreamMetrics(streamId),
}))

import type { ChartPoint } from '../hooks/useStreamMetrics'
import type { StreamMetricSnapshot } from '../types/stream.types'

function makeSnapshot(overrides?: Partial<StreamMetricSnapshot>): StreamMetricSnapshot {
  return {
    streamId: 'stream-001',
    streamName: 'Tech Talk Live',
    liveViewerCount: 847_230,
    viewerDelta: 1_230,
    bufferRatePct: 1.8,
    p95LatencyMs: 42,
    qualityDistribution: { '1080p': 45, '720p': 30, '480p': 25 },
    healthScore: 99.2,
    circuitBreakerState: 'CLOSED',
    activeAlerts: 0,
    snapshotTs: 1_717_350_000_000,
    ...overrides,
  }
}

function makeHistory(count: number): ChartPoint[] {
  return Array.from({ length: count }, (_, i) => ({
    time: `12:00:${String(i).padStart(2, '0')}`,
    ts: 1_717_350_000_000 + i * 1000,
    liveViewerCount: 847_230 + i,
  }))
}

afterEach(() => {
  mockUseStreamMetrics.mockReset()
})

describe('StreamCard', () => {
  it('renders stream name when snapshot is available', () => {
    mockUseStreamMetrics.mockReturnValue({
      snapshot: makeSnapshot(),
      history: makeHistory(10),
      connected: true,
    })

    render(<StreamCard streamId="stream-001" />)
    expect(screen.getByText('Tech Talk Live')).toBeDefined()
  })

  it('formats viewer count with thousands separator', () => {
    mockUseStreamMetrics.mockReturnValue({
      snapshot: makeSnapshot({ liveViewerCount: 847_230 }),
      history: [],
      connected: true,
    })

    render(<StreamCard streamId="stream-001" />)
    // Intl.NumberFormat produces "847,230" in en-US locale (jsdom default).
    expect(screen.getByText('847,230')).toBeDefined()
  })

  it('shows positive delta arrow when viewerDelta > 0', () => {
    mockUseStreamMetrics.mockReturnValue({
      snapshot: makeSnapshot({ viewerDelta: 1_230 }),
      history: [],
      connected: true,
    })

    render(<StreamCard streamId="stream-001" />)
    // Delta label includes "▲" for positive delta.
    const deltaEl = screen.getByText(/▲/)
    expect(deltaEl).toBeDefined()
  })

  it('shows negative delta arrow when viewerDelta < 0', () => {
    mockUseStreamMetrics.mockReturnValue({
      snapshot: makeSnapshot({ viewerDelta: -500 }),
      history: [],
      connected: true,
    })

    render(<StreamCard streamId="stream-001" />)
    const deltaEl = screen.getByText(/▼/)
    expect(deltaEl).toBeDefined()
  })

  it('shows buffer rate in the badge (SPEC-15 R3)', () => {
    mockUseStreamMetrics.mockReturnValue({
      snapshot: makeSnapshot({ bufferRatePct: 3.5 }),
      history: [],
      connected: true,
    })

    render(<StreamCard streamId="stream-001" />)
    // BufferRateBadge renders "3.5%" as a text node; regex matches it.
    expect(screen.getByText(/3\.5%/)).toBeDefined()
  })

  it('shows dash placeholder for viewer count when snapshot is null', () => {
    mockUseStreamMetrics.mockReturnValue({
      snapshot: null,
      history: [],
      connected: true,
    })

    render(<StreamCard streamId="stream-001" />)
    // Viewer count shows "—" before first message; badge is not rendered when null.
    expect(screen.getByText('—')).toBeDefined()
  })

  it('does not render BufferRateBadge when snapshot is null', () => {
    mockUseStreamMetrics.mockReturnValue({
      snapshot: null,
      history: [],
      connected: true,
    })

    render(<StreamCard streamId="stream-001" />)
    // Badge aria-label is not in DOM when snapshot is null.
    expect(screen.queryByLabelText(/Buffer rate:/)).toBeNull()
  })

  it('shows Reconnecting overlay when disconnected', () => {
    mockUseStreamMetrics.mockReturnValue({
      snapshot: makeSnapshot(),
      history: makeHistory(5),
      connected: false,
    })

    render(<StreamCard streamId="stream-001" />)
    expect(screen.getByText('Reconnecting…')).toBeDefined()
  })

  it('does NOT show Reconnecting overlay when connected', () => {
    mockUseStreamMetrics.mockReturnValue({
      snapshot: makeSnapshot(),
      history: makeHistory(5),
      connected: true,
    })

    render(<StreamCard streamId="stream-001" />)
    expect(screen.queryByText('Reconnecting…')).toBeNull()
  })

  it('uses streamName prop over snapshot name', () => {
    mockUseStreamMetrics.mockReturnValue({
      snapshot: makeSnapshot({ streamName: 'Snapshot Name' }),
      history: [],
      connected: true,
    })

    render(<StreamCard streamId="stream-001" streamName="Prop Name" />)
    expect(screen.getByText('Prop Name')).toBeDefined()
    expect(screen.queryByText('Snapshot Name')).toBeNull()
  })

  it('falls back to streamId when no name is available', () => {
    mockUseStreamMetrics.mockReturnValue({
      snapshot: null,
      history: [],
      connected: true,
    })

    render(<StreamCard streamId="stream-999" />)
    // stream-999 appears in both the display name span and the id badge span.
    const els = screen.getAllByText('stream-999')
    expect(els.length).toBeGreaterThanOrEqual(1)
  })

  it('applies fading class when fading prop is true', () => {
    mockUseStreamMetrics.mockReturnValue({
      snapshot: null,
      history: [],
      connected: true,
    })

    render(<StreamCard streamId="stream-001" fading={true} />)
    const card = screen.getByTestId('stream-card-stream-001')
    expect(card.className).toContain('opacity-0')
  })

  // ── SPEC-15 layout integration tests ──

  it('renders HealthGauge with aria-label when snapshot is available (SPEC-15 R1)', () => {
    mockUseStreamMetrics.mockReturnValue({
      snapshot: makeSnapshot({ healthScore: 95 }),
      history: [],
      connected: true,
    })

    render(<StreamCard streamId="stream-001" />)
    // HealthGauge sets role="img" with aria-label.
    const gauge = screen.getByRole('img', { name: /Health score:/ })
    expect(gauge).toBeDefined()
  })

  it('renders QualityDistBar when snapshot is available (SPEC-15 R2)', () => {
    mockUseStreamMetrics.mockReturnValue({
      snapshot: makeSnapshot(),
      history: [],
      connected: true,
    })

    render(<StreamCard streamId="stream-001" />)
    const bar = screen.getByRole('img', { name: 'Quality distribution bar' })
    expect(bar).toBeDefined()
  })

  it('renders BufferRateBadge with aria-label when snapshot is available (SPEC-15 R3)', () => {
    mockUseStreamMetrics.mockReturnValue({
      snapshot: makeSnapshot({ bufferRatePct: 1.8 }),
      history: [],
      connected: true,
    })

    render(<StreamCard streamId="stream-001" />)
    const badge = screen.getByLabelText(/Buffer rate: 1\.8 percent/)
    expect(badge).toBeDefined()
  })

  it('HealthGauge shows score 99 in the center for high-health stream (SPEC-15 R1)', () => {
    mockUseStreamMetrics.mockReturnValue({
      snapshot: makeSnapshot({ healthScore: 99.2 }),
      history: [],
      connected: true,
    })

    render(<StreamCard streamId="stream-001" />)
    // HealthGauge renders toFixed(0) → "99"
    expect(screen.getByText('99')).toBeDefined()
  })

  it('renders Quality section label (SPEC-15 R4 row 3 label)', () => {
    mockUseStreamMetrics.mockReturnValue({
      snapshot: makeSnapshot(),
      history: [],
      connected: true,
    })

    render(<StreamCard streamId="stream-001" />)
    expect(screen.getByText('Quality')).toBeDefined()
  })
})
