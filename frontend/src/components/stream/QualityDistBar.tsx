import React from 'react'
import {
  BarChart,
  Bar,
  XAxis,
  Tooltip,
  ResponsiveContainer,
  LabelList,
} from 'recharts'
import type { LabelFormatter } from 'recharts/types/component/Label'

/**
 * Quality tiers ordered from highest to lowest resolution.
 * Spec ref: SPEC-15 R2; project plan §5 data model.
 */
const QUALITY_TIERS: Array<{ key: string; color: string; label: string }> = [
  { key: '1080p', color: '#6366f1', label: '1080p' }, // indigo-500
  { key: '720p', color: '#22c55e', label: '720p' },   // green-500
  { key: '480p', color: '#eab308', label: '480p' },   // yellow-500
  { key: '360p', color: '#f97316', label: '360p' },   // orange-500
  { key: '144p', color: '#ef4444', label: '144p' },   // red-500
]

/** Minimum percentage to show a segment label (avoids label overlap on tiny slices). */
const LABEL_MIN_PCT = 8

interface QualityDistBarProps {
  /**
   * Map of quality label → percentage (should sum to ~100).
   * Missing keys default to 0.
   */
  distribution: Record<string, number>
}

/**
 * Build a label formatter for the given tier label.
 * Returns the tier label string when the bar value exceeds LABEL_MIN_PCT,
 * otherwise returns an empty string so Recharts renders nothing.
 *
 * Typed as LabelFormatter (label: RenderableText → RenderableText).
 */
function makeLabelFormatter(tierLabel: string): LabelFormatter {
  return (v) => {
    const n = typeof v === 'number' ? v : parseFloat(String(v ?? '0'))
    return n >= LABEL_MIN_PCT ? tierLabel : ''
  }
}

/**
 * QualityDistBar — 100%-stacked horizontal bar showing quality distribution
 * across 5 resolution tiers.  Tooltip shows raw percentages; labels appear
 * inside segments that are wide enough.
 *
 * Memoized — only re-renders when `distribution` changes by reference.
 *
 * Spec ref: SPEC-15 R2, R5, NFR1.
 */
export const QualityDistBar = React.memo(function QualityDistBar({
  distribution,
}: QualityDistBarProps) {
  // Normalize to a single-row data array that Recharts BarChart can consume.
  const row: Record<string, number | string> = { name: 'quality' }
  let total = 0
  for (const tier of QUALITY_TIERS) {
    const pct = distribution[tier.key] ?? 0
    row[tier.key] = pct
    total += pct
  }

  // If all zeroes (no data yet), render a placeholder track.
  const isEmpty = total === 0

  const data = [row]

  return (
    <div
      className="w-full"
      aria-label="Quality distribution bar"
      role="img"
    >
      <ResponsiveContainer width="100%" height={32}>
        <BarChart
          data={data}
          layout="vertical"
          margin={{ top: 0, right: 0, bottom: 0, left: 0 }}
          barCategoryGap={0}
        >
          <XAxis
            type="number"
            domain={[0, isEmpty ? 100 : 'auto']}
            hide
          />
          {/* No YAxis needed for a single-row bar */}
          <Tooltip
            cursor={false}
            contentStyle={{
              backgroundColor: '#111827',
              border: '1px solid #374151',
              borderRadius: '6px',
              fontSize: '12px',
              color: '#f9fafb',
            }}
            /* Recharts Tooltip formatter generic TValue resolves to ValueType which
               includes arrays; casting is safe here — our dataKeys are always numbers. */
            formatter={
              ((value: unknown, name: unknown) => [
                typeof value === 'number' ? `${value.toFixed(1)}%` : `${String(value)}%`,
                String(name),
              ]) as Parameters<typeof Tooltip>[0]['formatter']
            }
          />
          {isEmpty ? (
            /* Gray placeholder when no data received yet */
            <Bar dataKey="__empty__" fill="#1f2937" radius={[2, 2, 2, 2]} stackId="q" />
          ) : (
            QUALITY_TIERS.map((tier, idx) => (
              <Bar
                key={tier.key}
                dataKey={tier.key}
                stackId="q"
                fill={tier.color}
                isAnimationActive={false}
                radius={
                  idx === 0
                    ? [2, 0, 0, 2]
                    : idx === QUALITY_TIERS.length - 1
                      ? [0, 2, 2, 0]
                      : [0, 0, 0, 0]
                }
              >
                <LabelList
                  dataKey={tier.key}
                  position="insideLeft"
                  style={{ fontSize: '9px', fill: '#fff', fontWeight: 600 }}
                  formatter={makeLabelFormatter(tier.label)}
                />
              </Bar>
            ))
          )}
        </BarChart>
      </ResponsiveContainer>

      {/* Legend dots — color is not the sole indicator (accessibility). */}
      <div className="flex flex-wrap gap-x-3 gap-y-0.5 mt-1">
        {QUALITY_TIERS.map((tier) => {
          const pct = (distribution[tier.key] ?? 0).toFixed(1)
          return (
            <span key={tier.key} className="flex items-center gap-0.5 text-[9px] text-gray-400">
              <span
                className="inline-block w-2 h-2 rounded-sm shrink-0"
                style={{ backgroundColor: tier.color }}
              />
              {tier.label} {pct}%
            </span>
          )
        })}
      </div>
    </div>
  )
})
