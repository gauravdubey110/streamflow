/**
 * AlertBadge — severity color chip for alert rows in the AlertFeed.
 *
 * Colors:
 *   CRITICAL → red
 *   WARNING  → yellow/amber
 *   INFO     → blue
 *
 * Spec ref: SPEC-16 R2.
 */
import clsx from 'clsx'
import type { AlertEvent } from '../../types/alert.types'

interface AlertBadgeProps {
  severity: AlertEvent['severity']
}

const SEVERITY_STYLES: Record<AlertEvent['severity'], string> = {
  CRITICAL: 'bg-red-900/60 text-red-300 border border-red-700',
  WARNING: 'bg-amber-900/60 text-amber-300 border border-amber-700',
  INFO: 'bg-blue-900/60 text-blue-300 border border-blue-700',
}

export function AlertBadge({ severity }: AlertBadgeProps) {
  return (
    <span
      className={clsx(
        'inline-flex items-center rounded px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wide shrink-0',
        SEVERITY_STYLES[severity],
      )}
      aria-label={`Severity: ${severity}`}
    >
      {severity}
    </span>
  )
}
