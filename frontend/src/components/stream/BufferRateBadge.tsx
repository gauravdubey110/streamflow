import React from 'react'
import clsx from 'clsx'

/**
 * Color thresholds for buffer rate.
 * Spec ref: SPEC-15 R3 — green (<2), yellow (2–5), red (>5).
 */
function colorClasses(rate: number): {
  bg: string
  text: string
  pulse: boolean
} {
  if (rate > 5) {
    return { bg: 'bg-red-950', text: 'text-red-400', pulse: true }
  }
  if (rate >= 2) {
    return { bg: 'bg-yellow-950', text: 'text-yellow-400', pulse: false }
  }
  return { bg: 'bg-green-950', text: 'text-green-400', pulse: false }
}

interface BufferRateBadgeProps {
  /** Buffer rate as a percentage (e.g. 1.8 means 1.8%). */
  rate: number
}

/**
 * BufferRateBadge — pill showing buffer rate percentage.
 * - Green  when rate < 2 %
 * - Yellow when rate 2–5 %
 * - Red + subtle pulse when rate > 5 %
 *
 * The pulse is suppressed via `motion-reduce:animate-none` for users who
 * prefer reduced motion (SPEC-15 §4 Design Notes).
 *
 * Color is NOT the sole indicator — the numeric value and "buf" label are
 * always shown in text (SPEC-15 §6 Task 4 — accessibility).
 *
 * Memoized — only re-renders when `rate` changes.
 *
 * Spec ref: SPEC-15 R3, R5, NFR1.
 */
export const BufferRateBadge = React.memo(function BufferRateBadge({
  rate,
}: BufferRateBadgeProps) {
  const { bg, text, pulse } = colorClasses(rate)

  const label = `Buffer rate: ${rate.toFixed(1)} percent`

  return (
    <span
      className={clsx(
        'inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-semibold tabular-nums',
        bg,
        text,
        pulse && 'animate-pulse motion-reduce:animate-none',
      )}
      aria-label={label}
      title={label}
    >
      {rate.toFixed(1)}%
      <span className="font-normal opacity-70">buf</span>
    </span>
  )
})
