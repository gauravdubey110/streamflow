/**
 * useStreamHistory — fetches historical metric snapshots + alerts for a stream.
 *
 * Features:
 * - Fetches metrics and alerts in parallel via Promise.all.
 * - LRU cache (max 5 entries) keyed by (streamId,from,to,granularity) to make
 *   rapid range tweaks snappy without extra network calls.
 * - Returns {loading, error, data, alerts}.
 *
 * Spec ref: SPEC-19 R3, §4 Design Notes (LRU cache).
 */
import { useState, useCallback, useRef } from 'react'
import { fetchStreamHistory, fetchStreamAlerts } from '../services/api'
import type { HistoryPoint, HistoryGranularity } from '../types/stream.types'
import type { AlertEvent } from '../types/alert.types'

export interface UseStreamHistoryParams {
  streamId: string
  from: number
  to: number
  granularity: HistoryGranularity
}

export interface UseStreamHistoryResult {
  loading: boolean
  error: string | null
  data: HistoryPoint[]
  alerts: AlertEvent[]
  fetch: (params: UseStreamHistoryParams) => Promise<void>
}

/** Maximum entries to keep in the LRU cache. */
const LRU_MAX = 5

interface CacheEntry {
  data: HistoryPoint[]
  alerts: AlertEvent[]
}

function cacheKey(params: UseStreamHistoryParams): string {
  return `${params.streamId}|${params.from}|${params.to}|${params.granularity}`
}

/**
 * useStreamHistory — imperative fetch hook.
 *
 * Callers invoke `fetch(params)` on form submit; the hook manages loading, error,
 * and result state.  The LRU cache prevents duplicate network calls when the user
 * tweaks the same range repeatedly.
 */
export function useStreamHistory(): UseStreamHistoryResult {
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [data, setData] = useState<HistoryPoint[]>([])
  const [alerts, setAlerts] = useState<AlertEvent[]>([])

  // LRU cache: insertion-order Map, evict oldest when > LRU_MAX.
  const cacheRef = useRef<Map<string, CacheEntry>>(new Map())

  const fetchHistory = useCallback(async (params: UseStreamHistoryParams) => {
    const key = cacheKey(params)
    const cached = cacheRef.current.get(key)
    if (cached !== undefined) {
      // Move to end (most-recently-used).
      cacheRef.current.delete(key)
      cacheRef.current.set(key, cached)
      setData(cached.data)
      setAlerts(cached.alerts)
      setError(null)
      return
    }

    setLoading(true)
    setError(null)

    try {
      const [historyResult, alertsResult] = await Promise.all([
        fetchStreamHistory(params.streamId, params.from, params.to, params.granularity),
        fetchStreamAlerts(params.streamId, params.from, params.to),
      ])

      setData(historyResult)
      setAlerts(alertsResult)

      // Insert into LRU cache; evict oldest if over limit.
      const cache = cacheRef.current
      if (cache.size >= LRU_MAX) {
        const oldestKey = cache.keys().next().value
        if (oldestKey !== undefined) {
          cache.delete(oldestKey)
        }
      }
      cache.set(key, { data: historyResult, alerts: alertsResult })
    } catch (err: unknown) {
      const message =
        err instanceof Error ? err.message : 'Failed to load historical data'
      setError(message)
      setData([])
      setAlerts([])
    } finally {
      setLoading(false)
    }
  }, [])

  return { loading, error, data, alerts, fetch: fetchHistory }
}
