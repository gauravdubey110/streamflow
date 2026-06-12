import { describe, it, expect, beforeEach } from 'vitest'
import { useStreamStore } from '../store/streamStore'
import type { StreamMetricSnapshot } from '../types/stream.types'

const makeSnapshot = (overrides?: Partial<StreamMetricSnapshot>): StreamMetricSnapshot => ({
  streamId: 'stream-001',
  streamName: 'Tech Talk Live',
  liveViewerCount: 50000,
  viewerDelta: 100,
  bufferRatePct: 1.5,
  p95LatencyMs: 42,
  qualityDistribution: { '1080p': 45, '720p': 30, '480p': 25 },
  healthScore: 99.0,
  circuitBreakerState: 'CLOSED',
  activeAlerts: 0,
  snapshotTs: 1717350000000,
  ...overrides,
})

describe('streamStore', () => {
  beforeEach(() => {
    // Reset store state between tests.
    useStreamStore.setState({ streams: {} })
  })

  it('setSnapshot inserts a snapshot keyed by streamId', () => {
    const snap = makeSnapshot()
    useStreamStore.getState().setSnapshot(snap)

    const stored = useStreamStore.getState().streams['stream-001']
    expect(stored).toBeDefined()
    expect(stored?.streamId).toBe('stream-001')
    expect(stored?.liveViewerCount).toBe(50000)
  })

  it('setSnapshot upserts an existing entry', () => {
    const original = makeSnapshot({ liveViewerCount: 50000 })
    useStreamStore.getState().setSnapshot(original)

    const updated = makeSnapshot({ liveViewerCount: 75000 })
    useStreamStore.getState().setSnapshot(updated)

    const stored = useStreamStore.getState().streams['stream-001']
    expect(stored?.liveViewerCount).toBe(75000)
  })

  it('setSnapshot stores multiple streams independently', () => {
    useStreamStore.getState().setSnapshot(makeSnapshot({ streamId: 'stream-001' }))
    useStreamStore.getState().setSnapshot(makeSnapshot({ streamId: 'stream-002' }))

    const { streams } = useStreamStore.getState()
    expect(Object.keys(streams)).toHaveLength(2)
    expect(streams['stream-001']?.streamId).toBe('stream-001')
    expect(streams['stream-002']?.streamId).toBe('stream-002')
  })
})
