import { useEffect, useRef, useState, useCallback } from 'react'
import { useWebSocket } from './useWebSocket'
import type { MetricsUpdateMessage, StreamMetricSnapshot } from '../types/stream.types'

/** One point plotted on the ViewerCountChart. */
export interface ChartPoint {
  /** Formatted label for the x-axis: HH:mm:ss */
  time: string
  /** Raw epoch ms — used for dedup / ordering */
  ts: number
  liveViewerCount: number
}

/** Maximum number of data points kept in the chart history buffer. */
const HISTORY_LIMIT = 60

function formatTime(ts: number): string {
  return new Date(ts).toLocaleTimeString('en-GB', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  })
}

export interface UseStreamMetricsResult {
  /** Latest snapshot received from the WebSocket. Null before first message. */
  snapshot: StreamMetricSnapshot | null
  /** Ring buffer of the last HISTORY_LIMIT viewer-count data points. */
  history: ChartPoint[]
  /** Whether the global WebSocket is currently connected. */
  connected: boolean
}

/**
 * useStreamMetrics — subscribes to `/topic/streams/{streamId}/metrics` and
 * maintains a bounded ring buffer of the last 60 viewer-count points.
 *
 * The ring buffer is stored in a `useRef` to avoid a new array reference on
 * every push.  A derived `history` state (same array reference while unchanged)
 * is flushed via `setState` on each new point, which is the only render trigger.
 *
 * Spec ref: SPEC-08 R2, R3, R4, R5, Design Notes.
 */
export function useStreamMetrics(streamId: string): UseStreamMetricsResult {
  const { connected, subscribe, unsubscribe } = useWebSocket()

  const [snapshot, setSnapshot] = useState<StreamMetricSnapshot | null>(null)
  const [history, setHistory] = useState<ChartPoint[]>([])

  // The ring buffer lives in a ref so pushes don't trigger extra renders.
  const bufferRef = useRef<ChartPoint[]>([])

  const handleMessage = useCallback(
    (raw: unknown) => {
      const msg = raw as MetricsUpdateMessage

      // Upsert snapshot — shape mirrors MetricsUpdateMessage + defaults.
      const next: StreamMetricSnapshot = {
        streamId: msg.streamId,
        streamName: null, // populated later from REST discovery
        liveViewerCount: msg.liveViewerCount,
        viewerDelta: msg.viewerDelta,
        bufferRatePct: msg.bufferRatePct,
        p95LatencyMs: msg.p95LatencyMs,
        qualityDistribution: msg.qualityDistribution,
        healthScore: msg.healthScore,
        circuitBreakerState: 'CLOSED',
        activeAlerts: 0,
        snapshotTs: msg.ts,
      }
      setSnapshot(next)

      // Push to ring buffer; trim to HISTORY_LIMIT.
      const point: ChartPoint = {
        time: formatTime(msg.ts),
        ts: msg.ts,
        liveViewerCount: msg.liveViewerCount,
      }
      const buf = bufferRef.current
      buf.push(point)
      if (buf.length > HISTORY_LIMIT) {
        buf.shift()
      }

      // Shallow-copy so React detects the new array reference.
      setHistory([...buf])
    },
    [], // stable — streamId captured at subscription time
  )

  useEffect(() => {
    const destination = `/topic/streams/${streamId}/metrics`

    const subId = subscribe(destination, (message) => {
      try {
        handleMessage(JSON.parse(message.body) as unknown)
      } catch (err) {
        console.error('[useStreamMetrics] Failed to parse message:', err)
      }
    })

    return () => {
      unsubscribe(subId)
      // Clear buffer so stale data does not bleed into a new subscription.
      bufferRef.current = []
    }
    // subscribe/unsubscribe are stable (useCallback); streamId changes trigger resub.
  }, [streamId, subscribe, unsubscribe, handleMessage])

  return { snapshot, history, connected }
}
