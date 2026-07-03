import { useEffect, useRef, useState } from 'react'
import { Toaster } from 'react-hot-toast'
import { LiveDot } from './components/common/LiveDot'
import { StreamGrid } from './components/layout/StreamGrid'
import { useWebSocket } from './hooks/useWebSocket'
import { fetchStreams } from './services/api'
import type { StreamSummaryDTO } from './types/stream.types'

/**
 * How often (ms) to re-poll GET /api/v1/streams for new or gone streams.
 * Spec ref: SPEC-08 R7.
 */
const STREAM_POLL_INTERVAL_MS = 30_000

/**
 * How long (ms) a departed stream's card remains visible while fading out.
 * Spec ref: SPEC-08 R7.
 */
const FADE_DURATION_MS = 1_000

/**
 * App — root component.
 *
 * On mount and every 30s: polls GET /api/v1/streams to discover active streams.
 * New streams are added immediately; removed streams fade out over 1s before
 * being removed from the grid.
 *
 * Spec ref: SPEC-08 R1, R6, R7.
 */
function App() {
  const { connected } = useWebSocket()

  // Full list of streams currently displayed (including fading ones).
  const [streams, setStreams] = useState<StreamSummaryDTO[]>([])
  // Set of streamIds currently fading out.
  const [fadingIds, setFadingIds] = useState<Set<string>>(new Set())

  // Track current stream IDs so the effect closure always sees the latest.
  const streamIdsRef = useRef<Set<string>>(new Set())
  // Timeout handles for fading streams, keyed by streamId.
  const fadeTimersRef = useRef<Map<string, ReturnType<typeof setTimeout>>>(new Map())

  const handleFetchResult = (fetched: StreamSummaryDTO[]) => {
    const fetchedIds = new Set(fetched.map((s) => s.streamId))

    setStreams((prev) => {
      const prevIds = new Set(prev.map((s) => s.streamId))

      // Determine newly disappeared IDs.
      const disappeared = [...prevIds].filter((id) => !fetchedIds.has(id))

      // Mark them as fading.
      if (disappeared.length > 0) {
        setFadingIds((f) => {
          const next = new Set(f)
          disappeared.forEach((id) => next.add(id))
          return next
        })

        // Schedule removal after the CSS fade completes.
        disappeared.forEach((id) => {
          if (!fadeTimersRef.current.has(id)) {
            const handle = setTimeout(() => {
              setStreams((s) => s.filter((x) => x.streamId !== id))
              setFadingIds((f) => {
                const next = new Set(f)
                next.delete(id)
                return next
              })
              fadeTimersRef.current.delete(id)
              streamIdsRef.current.delete(id)
            }, FADE_DURATION_MS)
            fadeTimersRef.current.set(id, handle)
          }
        })
      }

      // Merge: keep existing entries (preserves order), add new ones.
      const existingMap = new Map(prev.map((s) => [s.streamId, s]))
      fetched.forEach((s) => existingMap.set(s.streamId, s)) // upsert
      streamIdsRef.current = fetchedIds

      return [...existingMap.values()]
    })
  }

  useEffect(() => {
    let cancelled = false
    // Capture ref value at effect-setup time so the cleanup closure uses a stable reference.
    // This satisfies react-hooks/exhaustive-deps for ref access inside cleanup — SPEC-08 NFR1.
    const fadeTimers = fadeTimersRef.current

    const poll = async () => {
      try {
        const fetched = await fetchStreams()
        if (!cancelled) handleFetchResult(fetched)
      } catch (err) {
        // Non-fatal: API may not be running during local dev; log and retry.
        console.warn('[App] fetchStreams failed:', err)
      }
    }

    // Immediate call on mount.
    void poll()

    const timer = setInterval(() => void poll(), STREAM_POLL_INTERVAL_MS)

    return () => {
      cancelled = true
      clearInterval(timer)
      fadeTimers.forEach((h) => clearTimeout(h))
      fadeTimers.clear()
    }
  }, [])

  return (
    <div className="min-h-screen bg-gray-950 text-white flex flex-col">
      {/* Toast notifications — SPEC-16 R7 */}
      <Toaster
        position="top-right"
        toastOptions={{
          style: {
            background: '#1f2937',
            color: '#f9fafb',
            border: '1px solid #374151',
            fontSize: '0.8rem',
          },
        }}
      />

      {/* Header */}
      <header className="border-b border-gray-800 px-6 py-3 flex items-center gap-3">
        <h1 className="text-xl font-bold tracking-tight">StreamFlow</h1>
        <div className="flex items-center gap-1.5">
          <LiveDot connected={connected} />
          <span className="text-xs text-gray-400">
            {connected ? 'live' : 'disconnected'}
          </span>
        </div>
      </header>

      {/* Main content */}
      <main className="flex-1 p-6">
        <StreamGrid streams={streams} fadingIds={fadingIds} />
      </main>
    </div>
  )
}

export default App
