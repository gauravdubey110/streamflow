import clsx from 'clsx'
import type { ReactNode } from 'react'

interface MetricCardProps {
  label: string
  value: string | number
  /** Optional secondary content rendered below the value. */
  sub?: ReactNode
  /** Extra Tailwind class names for the root element. */
  className?: string
}

/**
 * MetricCard — generic stat tile used inside StreamCard.
 *
 * Spec ref: SPEC-08 §6 (StreamCard sub-components).
 */
export function MetricCard({ label, value, sub, className }: MetricCardProps) {
  return (
    <div className={clsx('flex flex-col gap-0.5', className)}>
      <span className="text-xs uppercase tracking-wide text-gray-400">{label}</span>
      <span className="text-xl font-semibold tabular-nums text-white">{value}</span>
      {sub !== undefined && <span className="text-xs text-gray-500">{sub}</span>}
    </div>
  )
}
