/**
 * ChaosButton — "Inject Chaos" control with scenario + duration selection.
 *
 * Behaviour:
 * - Two selects: scenario (VIEWER_DROP | BITRATE_SPIKE | HIGH_BUFFER | STREAM_DOWN)
 *   and duration (10s | 30s | 60s).
 * - On "Inject" click → POST /api/v1/streams/{streamId}/chaos.
 * - While chaos is active: button is disabled; a client-side countdown shows
 *   remaining seconds derived from `startsAt + durationSeconds`.
 * - Toast on start success, cancel, and error (react-hot-toast).
 * - "Cancel" button appears during active chaos → calls DELETE chaos endpoint.
 *
 * Spec ref: SPEC-16 R5, R7.
 */
import { useState, useEffect, useRef } from 'react'
import toast from 'react-hot-toast'
import { injectChaos, cancelChaos } from '../../services/api'
import type { ChaosScenario } from '../../services/api'

interface ChaosButtonProps {
  streamId: string
}

const SCENARIOS: { value: ChaosScenario; label: string }[] = [
  { value: 'VIEWER_DROP', label: 'Viewer Drop' },
  { value: 'BITRATE_SPIKE', label: 'Bitrate Spike' },
  { value: 'HIGH_BUFFER', label: 'High Buffer' },
  { value: 'STREAM_DOWN', label: 'Stream Down' },
]

const DURATIONS: { value: number; label: string }[] = [
  { value: 10, label: '10s' },
  { value: 30, label: '30s' },
  { value: 60, label: '60s' },
]

export function ChaosButton({ streamId }: ChaosButtonProps) {
  const [scenario, setScenario] = useState<ChaosScenario>('HIGH_BUFFER')
  const [duration, setDuration] = useState(30)
  const [loading, setLoading] = useState(false)
  const [activeChaosId, setActiveChaosId] = useState<string | null>(null)
  const [countdown, setCountdown] = useState<number | null>(null)

  // Ref to hold the countdown interval so we can clear it on cancel/unmount.
  const countdownRef = useRef<ReturnType<typeof setInterval> | null>(null)
  // Ref to hold the endsAt epoch so interval can compute remaining time.
  const endsAtRef = useRef<number | null>(null)

  // Clear countdown interval helper.
  const clearCountdown = () => {
    if (countdownRef.current !== null) {
      clearInterval(countdownRef.current)
      countdownRef.current = null
    }
    endsAtRef.current = null
    setCountdown(null)
  }

  // Start countdown timer given the chaos end epoch.
  const startCountdown = (endsAt: number) => {
    endsAtRef.current = endsAt
    const tick = () => {
      const remaining = Math.max(0, Math.ceil((endsAtRef.current! - Date.now()) / 1000))
      setCountdown(remaining)
      if (remaining === 0) {
        clearCountdown()
        setActiveChaosId(null)
      }
    }
    tick()
    countdownRef.current = setInterval(tick, 1000)
  }

  // Cleanup on unmount.
  useEffect(() => {
    return () => {
      clearCountdown()
    }
  }, [])

  const handleInject = async () => {
    setLoading(true)
    try {
      const result = await injectChaos(streamId, scenario, duration)
      setActiveChaosId(result.chaosId)
      // Backend returns startsAt; add duration to compute endsAt.
      const endsAt = result.startsAt + duration * 1000
      startCountdown(endsAt)
      toast.success(`Chaos injected: ${scenario} for ${duration}s`)
    } catch (err) {
      console.error('[ChaosButton] injectChaos failed:', err)
      toast.error('Failed to inject chaos — API error')
    } finally {
      setLoading(false)
    }
  }

  const handleCancel = async () => {
    if (!activeChaosId) return
    setLoading(true)
    try {
      await cancelChaos(streamId, activeChaosId)
      clearCountdown()
      setActiveChaosId(null)
      toast.success('Chaos cancelled')
    } catch (err) {
      console.error('[ChaosButton] cancelChaos failed:', err)
      toast.error('Failed to cancel chaos — API error')
    } finally {
      setLoading(false)
    }
  }

  const isActive = activeChaosId !== null

  return (
    <div className="flex flex-wrap items-center gap-1.5" data-testid="chaos-button-group">
      {/* Scenario selector */}
      <select
        value={scenario}
        onChange={(e) => setScenario(e.target.value as ChaosScenario)}
        disabled={isActive || loading}
        className="rounded border border-gray-700 bg-gray-800 px-2 py-1 text-xs text-gray-200 focus:outline-none focus:ring-1 focus:ring-red-500 disabled:opacity-50"
        aria-label="Chaos scenario"
        data-testid="chaos-scenario-select"
      >
        {SCENARIOS.map((s) => (
          <option key={s.value} value={s.value}>
            {s.label}
          </option>
        ))}
      </select>

      {/* Duration selector */}
      <select
        value={duration}
        onChange={(e) => setDuration(Number(e.target.value))}
        disabled={isActive || loading}
        className="rounded border border-gray-700 bg-gray-800 px-2 py-1 text-xs text-gray-200 focus:outline-none focus:ring-1 focus:ring-red-500 disabled:opacity-50"
        aria-label="Chaos duration"
        data-testid="chaos-duration-select"
      >
        {DURATIONS.map((d) => (
          <option key={d.value} value={d.value}>
            {d.label}
          </option>
        ))}
      </select>

      {/* Inject / countdown button */}
      {!isActive ? (
        <button
          onClick={() => void handleInject()}
          disabled={loading}
          className="rounded bg-red-700 px-3 py-1 text-xs font-semibold text-white transition-colors hover:bg-red-600 disabled:opacity-50"
          aria-label="Inject chaos"
          data-testid="chaos-inject-btn"
        >
          {loading ? 'Injecting…' : 'Inject Chaos'}
        </button>
      ) : (
        <div className="flex items-center gap-1.5">
          <span
            className="rounded bg-red-900/60 border border-red-700 px-2 py-1 text-xs font-mono text-red-300"
            aria-live="polite"
            data-testid="chaos-countdown"
          >
            {countdown !== null ? `${countdown}s` : '—'}
          </span>
          <button
            onClick={() => void handleCancel()}
            disabled={loading}
            className="rounded border border-gray-600 bg-gray-800 px-2 py-1 text-xs text-gray-300 hover:bg-gray-700 disabled:opacity-50"
            aria-label="Cancel chaos"
            data-testid="chaos-cancel-btn"
          >
            {loading ? 'Cancelling…' : 'Cancel'}
          </button>
        </div>
      )}
    </div>
  )
}
