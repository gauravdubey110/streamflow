import clsx from 'clsx'
import { useStreamMetrics } from '../../hooks/useStreamMetrics'
import { LiveDot } from '../common/LiveDot'
import { MetricCard } from '../common/MetricCard'
import { ViewerCountChart } from './ViewerCountChart'

interface StreamCardProps {
  streamId: string
  /** Preferred display name from REST discovery; falls back to streamId. */
  streamName?: string | null
  /** When true, the card fades out (stream no longer returned by the API). */
  fading?: boolean
}

const fmt = new Intl.NumberFormat()

/**
 * StreamCard — tile for one live stream.  Subscribes to WebSocket metrics,
 * shows viewer count, delta arrow, buffer rate, and a 60-second chart.
 * Overlays a "Reconnecting…" pulse if the WebSocket connection is lost.
 *
 * Spec ref: SPEC-08 R2, R3, R5, R6.
 */
export function StreamCard({ streamId, streamName, fading = false }: StreamCardProps) {
  const { snapshot, history, connected } = useStreamMetrics(streamId)

  const displayName = streamName ?? snapshot?.streamName ?? streamId

  const viewerCount = snapshot ? fmt.format(snapshot.liveViewerCount) : '—'

  const deltaSign =
    snapshot && snapshot.viewerDelta > 0
      ? '+'
      : snapshot && snapshot.viewerDelta < 0
        ? ''
        : ''

  const deltaLabel =
    snapshot && snapshot.viewerDelta !== 0
      ? `${deltaSign}${fmt.format(snapshot.viewerDelta)} ${snapshot.viewerDelta > 0 ? '▲' : '▼'}`
      : null

  const deltaColor =
    snapshot && snapshot.viewerDelta > 0
      ? 'text-green-400'
      : snapshot && snapshot.viewerDelta < 0
        ? 'text-red-400'
        : 'text-gray-400'

  const bufferLabel =
    snapshot !== null ? `${snapshot.bufferRatePct.toFixed(1)}%` : '—'

  return (
    <div
      className={clsx(
        'relative rounded-xl border border-gray-800 bg-gray-900 p-4 flex flex-col gap-3 transition-opacity duration-1000',
        fading ? 'opacity-0' : 'opacity-100',
      )}
      data-testid={`stream-card-${streamId}`}
    >
      {/* Header row */}
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-2 min-w-0">
          <LiveDot connected={connected} />
          <span className="font-semibold text-sm text-white truncate" title={displayName}>
            {displayName}
          </span>
        </div>
        <span className="text-xs text-gray-500 shrink-0">{streamId}</span>
      </div>

      {/* Metrics row */}
      <div className="flex gap-4">
        <MetricCard
          label="Viewers"
          value={viewerCount}
          sub={
            deltaLabel !== null ? (
              <span className={deltaColor}>{deltaLabel}</span>
            ) : undefined
          }
          className="flex-1"
        />
        <MetricCard label="Buffer rate" value={bufferLabel} className="shrink-0" />
      </div>

      {/* Chart */}
      <div className="mt-1" aria-label="Viewer count chart">
        <ViewerCountChart history={history} />
      </div>

      {/* Reconnecting overlay — spec R5 */}
      {!connected && (
        <div
          className="absolute inset-0 rounded-xl bg-gray-950/80 flex items-center justify-center"
          aria-live="polite"
          role="status"
        >
          <span className="animate-pulse text-sm text-gray-300">Reconnecting…</span>
        </div>
      )}
    </div>
  )
}
