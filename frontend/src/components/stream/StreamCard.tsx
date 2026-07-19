import clsx from 'clsx'
import { useState } from 'react'
import { useStreamMetrics } from '../../hooks/useStreamMetrics'
import { useAlerts } from '../../hooks/useAlerts'
import { LiveDot } from '../common/LiveDot'
import { MetricCard } from '../common/MetricCard'
import { BufferRateBadge } from './BufferRateBadge'
import { HealthGauge } from './HealthGauge'
import { QualityDistBar } from './QualityDistBar'
import { ViewerCountChart } from './ViewerCountChart'
import { AlertFeed } from '../alerts/AlertFeed'
import { StreamControls } from '../controls/StreamControls'
import { HistoryModal } from '../history/HistoryModal'

interface StreamCardProps {
  streamId: string
  /** Preferred display name from REST discovery; falls back to streamId. */
  streamName?: string | null
  /** When true, the card fades out (stream no longer returned by the API). */
  fading?: boolean
}

const fmt = new Intl.NumberFormat('en-US')

/**
 * StreamCard — tile for one live stream.  Subscribes to WebSocket metrics.
 *
 * Layout (SPEC-15 R4):
 *   Row 1: stream name + LiveDot + BufferRateBadge
 *   Row 2: ViewerCountChart (full width)
 *   Row 3: HealthGauge | QualityDistBar (50/50 columns)
 *
 * Overlays a "Reconnecting…" pulse if the WebSocket connection is lost.
 *
 * Spec ref: SPEC-08 R2, R3, R5, R6; SPEC-15 R3, R4.
 */
export function StreamCard({ streamId, streamName, fading = false }: StreamCardProps) {
  const { snapshot, history, connected } = useStreamMetrics(streamId)
  const alerts = useAlerts(streamId)
  const [historyOpen, setHistoryOpen] = useState(false)

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

  const bufferRate = snapshot?.bufferRatePct ?? 0
  const healthScore = snapshot?.healthScore ?? 100
  const qualityDist = snapshot?.qualityDistribution ?? {}

  return (
    <div
      className={clsx(
        'relative rounded-xl border border-gray-800 bg-gray-900 p-4 flex flex-col gap-3 transition-opacity duration-1000',
        fading ? 'opacity-0' : 'opacity-100',
      )}
      data-testid={`stream-card-${streamId}`}
    >
      {/* ── Row 1: name + LiveDot + BufferRateBadge ── Spec-15 R4 */}
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-2 min-w-0">
          <LiveDot connected={connected} />
          <span className="font-semibold text-sm text-white truncate" title={displayName}>
            {displayName}
          </span>
          <span className="text-xs text-gray-500 shrink-0">{streamId}</span>
        </div>
        {snapshot !== null && (
          <BufferRateBadge rate={bufferRate} />
        )}
      </div>

      {/* ── Viewers metric ── */}
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
      </div>

      {/* ── Row 2: ViewerCountChart (full width) ── Spec-15 R4 */}
      <div aria-label="Viewer count chart">
        <ViewerCountChart history={history} />
      </div>

      {/* ── Row 3: HealthGauge | QualityDistBar (50/50) ── Spec-15 R4 */}
      <div className="flex items-center gap-3">
        <div className="flex flex-col items-center shrink-0">
          <HealthGauge score={healthScore} />
        </div>
        <div className="flex-1 min-w-0">
          <span className="text-xs uppercase tracking-wide text-gray-400 block mb-1">
            Quality
          </span>
          <QualityDistBar distribution={qualityDist} />
        </div>
      </div>

      {/* ── Row 4: Alert Feed ── Spec-16 R2 */}
      <div>
        <span className="text-xs uppercase tracking-wide text-gray-400 block mb-1">
          Alerts
        </span>
        <AlertFeed alerts={alerts} />
      </div>

      {/* ── Row 5: Stream Controls (CB indicator + Chaos button) ── Spec-16 R6 */}
      <StreamControls streamId={streamId} />

      {/* ── Row 6: History button ── Spec-19 R1 */}
      <div>
        <button
          onClick={() => setHistoryOpen(true)}
          className="text-xs text-gray-400 hover:text-blue-400 transition-colors focus:outline-none focus:ring-1 focus:ring-blue-500 rounded px-1"
          aria-label={`Open historical replay for ${displayName}`}
          data-testid={`history-btn-${streamId}`}
        >
          History
        </button>
      </div>

      {/* ── History Modal ── Spec-19 R1 */}
      {historyOpen && (
        <HistoryModal
          streamId={streamId}
          streamName={streamName ?? snapshot?.streamName}
          onClose={() => setHistoryOpen(false)}
        />
      )}

      {/* ── Reconnecting overlay — spec R5 ── */}
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
