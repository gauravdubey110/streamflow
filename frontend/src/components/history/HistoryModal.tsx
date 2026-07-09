/**
 * HistoryModal — date-range picker + historical replay chart for one stream.
 *
 * Features:
 * - Two datetime-local inputs (from / to), defaulting to the last 1 hour.
 * - Granularity toggle: MINUTE / HOUR. Auto-switches to HOUR when range > 2h.
 * - On submit: calls useStreamHistory hook to fetch metrics + alerts in parallel.
 * - Renders ReplayChart, or loading skeleton, or error toast, or empty state.
 * - Modal dialog: focus trap on open, ESC closes, aria-modal, aria-labelledby.
 *
 * Spec ref: SPEC-19 R1, R2, R4, R5, R6.
 */
import { useEffect, useRef, useCallback, useState } from 'react'
import toast from 'react-hot-toast'
import { useStreamHistory } from '../../hooks/useStreamHistory'
import { ReplayChart } from './ReplayChart'
import type { HistoryGranularity } from '../../types/stream.types'

interface HistoryModalProps {
  streamId: string
  streamName?: string | null
  onClose: () => void
}

/** Format a Date as a local datetime-local input value (YYYY-MM-DDTHH:mm). */
function toDatetimeLocal(d: Date): string {
  const pad = (n: number) => String(n).padStart(2, '0')
  return (
    `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}` +
    `T${pad(d.getHours())}:${pad(d.getMinutes())}`
  )
}

/** Parse a datetime-local string to epoch ms (local time). */
function fromDatetimeLocal(s: string): number {
  return new Date(s).getTime()
}

/** Compute the range in hours between two epoch ms values. */
function rangeHours(from: number, to: number): number {
  return (to - from) / 3_600_000
}

const MS_PER_HOUR = 3_600_000

export function HistoryModal({ streamId, streamName, onClose }: HistoryModalProps) {
  const now = new Date()
  const oneHourAgo = new Date(now.getTime() - MS_PER_HOUR)

  const [fromValue, setFromValue] = useState(() => toDatetimeLocal(oneHourAgo))
  const [toValue, setToValue] = useState(() => toDatetimeLocal(now))
  const [granularity, setGranularity] = useState<HistoryGranularity>('MINUTE')

  const { loading, error, data, alerts, fetch: fetchHistory } = useStreamHistory()

  const dialogRef = useRef<HTMLDivElement>(null)
  const titleId = `history-modal-title-${streamId}`

  // Focus trap: on open, focus the dialog container; on ESC, close.
  useEffect(() => {
    const el = dialogRef.current
    if (el === null) return
    el.focus()

    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        onClose()
        return
      }
      // Trap Tab within the dialog.
      if (e.key === 'Tab') {
        const focusable = el.querySelectorAll<HTMLElement>(
          'button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])',
        )
        const focusableArr = Array.from(focusable)
        if (focusableArr.length === 0) return
        const first: HTMLElement | undefined = focusableArr[0]
        const last: HTMLElement | undefined = focusableArr[focusableArr.length - 1]
        if (first === undefined || last === undefined) return
        if (e.shiftKey) {
          if (document.activeElement === first) {
            e.preventDefault()
            last.focus()
          }
        } else {
          if (document.activeElement === last) {
            e.preventDefault()
            first.focus()
          }
        }
      }
    }

    document.addEventListener('keydown', handleKeyDown)
    return () => document.removeEventListener('keydown', handleKeyDown)
  }, [onClose])

  // Show error toast when error changes.
  useEffect(() => {
    if (error !== null) {
      toast.error(`History load failed: ${error}`)
    }
  }, [error])

  // Auto-switch granularity when range changes.
  const handleFromChange = (v: string) => {
    setFromValue(v)
    const to = fromDatetimeLocal(toValue)
    const from = fromDatetimeLocal(v)
    if (!isNaN(from) && !isNaN(to) && rangeHours(from, to) > 2) {
      setGranularity('HOUR')
    } else {
      setGranularity('MINUTE')
    }
  }

  const handleToChange = (v: string) => {
    setToValue(v)
    const from = fromDatetimeLocal(fromValue)
    const to = fromDatetimeLocal(v)
    if (!isNaN(from) && !isNaN(to) && rangeHours(from, to) > 2) {
      setGranularity('HOUR')
    } else {
      setGranularity('MINUTE')
    }
  }

  const handleSubmit = useCallback(
    (e: React.FormEvent) => {
      e.preventDefault()
      const from = fromDatetimeLocal(fromValue)
      const to = fromDatetimeLocal(toValue)
      if (isNaN(from) || isNaN(to) || from >= to) {
        toast.error('Invalid date range: "From" must be before "To"')
        return
      }
      void fetchHistory({ streamId, from, to, granularity })
    },
    [fromValue, toValue, granularity, streamId, fetchHistory],
  )

  const displayName = streamName ?? streamId

  return (
    /* Backdrop */
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4"
      role="presentation"
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose()
      }}
      data-testid="history-modal-backdrop"
    >
      {/* Dialog */}
      <div
        ref={dialogRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        tabIndex={-1}
        className="relative w-full max-w-3xl rounded-xl border border-gray-700 bg-gray-900 p-6 shadow-2xl focus:outline-none"
        data-testid="history-modal"
      >
        {/* Header */}
        <div className="mb-4 flex items-center justify-between gap-2">
          <h2
            id={titleId}
            className="text-base font-semibold text-white"
          >
            Historical Replay — {displayName}
          </h2>
          <button
            onClick={onClose}
            aria-label="Close history modal"
            className="rounded p-1 text-gray-400 hover:bg-gray-800 hover:text-white focus:outline-none focus:ring-2 focus:ring-blue-500"
            data-testid="history-modal-close"
          >
            ✕
          </button>
        </div>

        {/* Controls form */}
        <form
          onSubmit={handleSubmit}
          className="mb-4 flex flex-wrap items-end gap-3"
          data-testid="history-form"
        >
          {/* From */}
          <div className="flex flex-col gap-1">
            <label
              htmlFor={`history-from-${streamId}`}
              className="text-xs text-gray-400"
            >
              From
            </label>
            <input
              id={`history-from-${streamId}`}
              type="datetime-local"
              value={fromValue}
              onChange={(e) => handleFromChange(e.target.value)}
              className="rounded border border-gray-700 bg-gray-800 px-2 py-1 text-xs text-gray-200 focus:outline-none focus:ring-1 focus:ring-blue-500"
              data-testid="history-from-input"
            />
          </div>

          {/* To */}
          <div className="flex flex-col gap-1">
            <label
              htmlFor={`history-to-${streamId}`}
              className="text-xs text-gray-400"
            >
              To
            </label>
            <input
              id={`history-to-${streamId}`}
              type="datetime-local"
              value={toValue}
              onChange={(e) => handleToChange(e.target.value)}
              className="rounded border border-gray-700 bg-gray-800 px-2 py-1 text-xs text-gray-200 focus:outline-none focus:ring-1 focus:ring-blue-500"
              data-testid="history-to-input"
            />
          </div>

          {/* Granularity toggle */}
          <div className="flex flex-col gap-1">
            <span className="text-xs text-gray-400">Granularity</span>
            <div className="flex rounded border border-gray-700 overflow-hidden">
              {(['MINUTE', 'HOUR'] as HistoryGranularity[]).map((g) => (
                <button
                  key={g}
                  type="button"
                  onClick={() => setGranularity(g)}
                  aria-pressed={granularity === g}
                  className={`px-3 py-1 text-xs transition-colors focus:outline-none focus:ring-1 focus:ring-blue-500 ${
                    granularity === g
                      ? 'bg-blue-700 text-white'
                      : 'bg-gray-800 text-gray-400 hover:bg-gray-700'
                  }`}
                  data-testid={`granularity-${g.toLowerCase()}`}
                >
                  {g}
                </button>
              ))}
            </div>
          </div>

          {/* Submit */}
          <button
            type="submit"
            disabled={loading}
            className="rounded bg-blue-700 px-4 py-1.5 text-xs font-semibold text-white hover:bg-blue-600 disabled:opacity-50 focus:outline-none focus:ring-2 focus:ring-blue-500"
            data-testid="history-submit-btn"
          >
            {loading ? 'Loading…' : 'Load'}
          </button>
        </form>

        {/* Chart area */}
        {loading ? (
          <div
            className="flex h-60 flex-col gap-2 animate-pulse"
            data-testid="history-loading-skeleton"
            aria-label="Loading historical data"
            role="status"
            aria-live="polite"
          >
            <div className="h-4 w-1/3 rounded bg-gray-800" />
            <div className="flex-1 rounded bg-gray-800" />
          </div>
        ) : (
          <ReplayChart data={data} alerts={alerts} />
        )}
      </div>
    </div>
  )
}
