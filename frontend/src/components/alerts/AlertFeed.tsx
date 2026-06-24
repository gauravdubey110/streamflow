/**
 * AlertFeed — scrolling list of alerts for a stream, newest on top.
 *
 * Each row shows: AlertBadge (severity) | alert type | message | relative time.
 * New items slide in from the top via a CSS keyframe animation.
 *
 * The list is capped at 100 displayed items for 60fps scroll performance (NFR1).
 *
 * Spec ref: SPEC-16 R2, NFR1.
 */
import { AlertBadge } from './AlertBadge'
import type { AlertEvent } from '../../types/alert.types'

interface AlertFeedProps {
  alerts: AlertEvent[]
}

const DISPLAY_LIMIT = 100

function formatRelativeTime(ts: number): string {
  const diffMs = Date.now() - ts
  const diffSec = Math.floor(diffMs / 1000)
  if (diffSec < 60) return `${diffSec}s ago`
  const diffMin = Math.floor(diffSec / 60)
  if (diffMin < 60) return `${diffMin}m ago`
  const diffHr = Math.floor(diffMin / 60)
  return `${diffHr}h ago`
}

function formatAlertType(alertType: string): string {
  return alertType.replace(/_/g, ' ')
}

export function AlertFeed({ alerts }: AlertFeedProps) {
  const visible = alerts.slice(0, DISPLAY_LIMIT)

  if (visible.length === 0) {
    return (
      <div
        className="text-xs text-gray-500 py-2 text-center"
        data-testid="alert-feed-empty"
      >
        No alerts
      </div>
    )
  }

  return (
    <ul
      className="flex flex-col gap-1 max-h-40 overflow-y-auto pr-1"
      aria-label="Alert feed"
      data-testid="alert-feed"
    >
      {visible.map((alert) => (
        <li
          key={alert.alertId}
          className="alert-slide-in flex items-start gap-1.5 rounded bg-gray-800/60 px-2 py-1.5 text-xs"
          data-testid={`alert-row-${alert.alertId}`}
        >
          <AlertBadge severity={alert.severity} />
          <span className="font-medium text-gray-200 shrink-0">
            {formatAlertType(alert.alertType)}
          </span>
          <span className="text-gray-400 truncate flex-1" title={alert.message}>
            {alert.message}
          </span>
          <span className="text-gray-500 shrink-0 tabular-nums">
            {formatRelativeTime(alert.timestamp)}
          </span>
        </li>
      ))}
    </ul>
  )
}
