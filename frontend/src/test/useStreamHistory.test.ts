/**
 * useStreamHistory hook tests.
 *
 * Spec ref: SPEC-19 R3, §4 (LRU cache), Test Plan.
 * Tests: parallel fetch, LRU cache hit avoids second call, loading/error states.
 */
import { describe, it, expect, vi, afterEach } from 'vitest'
import { renderHook, act } from '@testing-library/react'
import { useStreamHistory } from '../hooks/useStreamHistory'

// Mock the api module.
vi.mock('../services/api', () => ({
  fetchStreamHistory: vi.fn(),
  fetchStreamAlerts: vi.fn(),
}))

import { fetchStreamHistory, fetchStreamAlerts } from '../services/api'

const mockFetchHistory = vi.mocked(fetchStreamHistory)
const mockFetchAlerts = vi.mocked(fetchStreamAlerts)

function makeHistoryPoint(snapshotTs = Date.now()) {
  return {
    streamId: 'stream-001',
    snapshotTs,
    liveViewerCount: 10_000,
    bufferRatePct: 1.5,
    p95LatencyMs: 40,
    healthScore: 98,
    qualityDistribution: { '1080p': 50 },
  }
}

function makeAlert() {
  return {
    alertId: 'alert-1',
    streamId: 'stream-001',
    severity: 'WARNING' as const,
    alertType: 'HIGH_BUFFER_RATE' as const,
    threshold: 5,
    actualValue: 8,
    message: 'Buffer rate too high',
    timestamp: Date.now(),
  }
}

afterEach(() => {
  vi.clearAllMocks()
})

describe('useStreamHistory', () => {
  it('starts with empty data and no loading', () => {
    const { result } = renderHook(() => useStreamHistory())
    expect(result.current.loading).toBe(false)
    expect(result.current.error).toBeNull()
    expect(result.current.data).toEqual([])
    expect(result.current.alerts).toEqual([])
  })

  it('fetches metrics and alerts in parallel and updates state', async () => {
    const points = [makeHistoryPoint(), makeHistoryPoint(Date.now() + 60_000)]
    const alertList = [makeAlert()]

    mockFetchHistory.mockResolvedValueOnce(points)
    mockFetchAlerts.mockResolvedValueOnce(alertList)

    const { result } = renderHook(() => useStreamHistory())

    await act(async () => {
      await result.current.fetch({
        streamId: 'stream-001',
        from: 1_000_000,
        to: 2_000_000,
        granularity: 'MINUTE',
      })
    })

    expect(mockFetchHistory).toHaveBeenCalledWith('stream-001', 1_000_000, 2_000_000, 'MINUTE')
    expect(mockFetchAlerts).toHaveBeenCalledWith('stream-001', 1_000_000, 2_000_000)
    expect(result.current.data).toHaveLength(2)
    expect(result.current.alerts).toHaveLength(1)
    expect(result.current.loading).toBe(false)
    expect(result.current.error).toBeNull()
  })

  it('sets loading=true during fetch and loading=false after', async () => {
    let resolveHistory!: (v: ReturnType<typeof makeHistoryPoint>[]) => void
    const historyPromise = new Promise<ReturnType<typeof makeHistoryPoint>[]>((res) => {
      resolveHistory = res
    })
    mockFetchHistory.mockReturnValueOnce(historyPromise)
    mockFetchAlerts.mockResolvedValueOnce([])

    const { result } = renderHook(() => useStreamHistory())

    // Start fetch without awaiting.
    act(() => {
      void result.current.fetch({
        streamId: 'stream-001',
        from: 1_000_000,
        to: 2_000_000,
        granularity: 'MINUTE',
      })
    })

    // Loading should be true immediately.
    expect(result.current.loading).toBe(true)

    // Resolve the promise.
    await act(async () => {
      resolveHistory([makeHistoryPoint()])
    })

    expect(result.current.loading).toBe(false)
  })

  it('sets error state when API rejects', async () => {
    mockFetchHistory.mockRejectedValueOnce(new Error('Network error'))
    mockFetchAlerts.mockResolvedValueOnce([])

    const { result } = renderHook(() => useStreamHistory())

    await act(async () => {
      await result.current.fetch({
        streamId: 'stream-001',
        from: 1_000_000,
        to: 2_000_000,
        granularity: 'MINUTE',
      })
    })

    expect(result.current.error).toBe('Network error')
    expect(result.current.data).toEqual([])
    expect(result.current.loading).toBe(false)
  })

  it('returns cached result on second identical fetch (LRU hit)', async () => {
    const points = [makeHistoryPoint()]
    const alertList = [makeAlert()]

    mockFetchHistory.mockResolvedValueOnce(points)
    mockFetchAlerts.mockResolvedValueOnce(alertList)

    const { result } = renderHook(() => useStreamHistory())

    const params = {
      streamId: 'stream-001',
      from: 1_000_000,
      to: 2_000_000,
      granularity: 'MINUTE' as const,
    }

    // First fetch — hits network.
    await act(async () => {
      await result.current.fetch(params)
    })
    expect(mockFetchHistory).toHaveBeenCalledTimes(1)

    // Second fetch with same params — should hit cache.
    await act(async () => {
      await result.current.fetch(params)
    })
    expect(mockFetchHistory).toHaveBeenCalledTimes(1) // still only 1 call
    expect(result.current.data).toHaveLength(1)
  })

  it('does NOT hit cache for different params', async () => {
    mockFetchHistory.mockResolvedValue([])
    mockFetchAlerts.mockResolvedValue([])

    const { result } = renderHook(() => useStreamHistory())

    const params1 = { streamId: 'stream-001', from: 1_000_000, to: 2_000_000, granularity: 'MINUTE' as const }
    const params2 = { streamId: 'stream-001', from: 3_000_000, to: 4_000_000, granularity: 'MINUTE' as const }

    await act(async () => { await result.current.fetch(params1) })
    await act(async () => { await result.current.fetch(params2) })

    expect(mockFetchHistory).toHaveBeenCalledTimes(2)
  })
})
