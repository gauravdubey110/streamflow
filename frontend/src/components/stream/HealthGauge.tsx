import React from 'react'
import { RadialBarChart, RadialBar, PolarAngleAxis } from 'recharts'

/**
 * Color ramp for the health gauge arc.
 * Spec ref: SPEC-15 R1 — green ≥85, yellow 60–84, red <60.
 */
function gaugeColor(score: number): string {
  if (score >= 85) return '#22c55e' // health.good
  if (score >= 60) return '#eab308' // health.warn
  return '#ef4444' // health.bad
}

/**
 * Tailwind text-color class for the center score label.
 * Using inline style for the arc but text class for the overlay.
 * Spec ref: SPEC-15 R1 — color is not sole indicator; includes numeric text.
 */
function scoreTextClass(score: number): string {
  if (score >= 85) return 'text-health-good'
  if (score >= 60) return 'text-health-warn'
  return 'text-health-bad'
}

interface HealthGaugeProps {
  /** 0–100 health score. */
  score: number
}

const WIDTH = 120
const HEIGHT = 120

/**
 * HealthGauge — Recharts RadialBarChart showing a single arc for the health
 * score (0–100).  Color ramps green (≥85) → yellow (60–84) → red (<60).
 * Numeric score is shown in the center as a fallback for colorblind users.
 *
 * Memoized — only re-renders when `score` changes.
 *
 * Spec ref: SPEC-15 R1, R5, NFR1.
 */
export const HealthGauge = React.memo(function HealthGauge({ score }: HealthGaugeProps) {
  const clampedScore = Math.max(0, Math.min(100, score))
  const color = gaugeColor(clampedScore)
  const textClass = scoreTextClass(clampedScore)

  // Recharts RadialBarChart expects an array of data objects.
  const data = [{ name: 'health', value: clampedScore, fill: color }]

  return (
    <div
      className="relative flex items-center justify-center"
      style={{ width: WIDTH, height: HEIGHT }}
      aria-label={`Health score: ${clampedScore.toFixed(0)} out of 100`}
      role="img"
    >
      <RadialBarChart
        width={WIDTH}
        height={HEIGHT}
        innerRadius="65%"
        outerRadius="90%"
        data={data}
        startAngle={225}
        endAngle={-45}
        barSize={10}
      >
        {/* Domain 0–100 so value maps correctly to arc position. */}
        <PolarAngleAxis type="number" domain={[0, 100]} angleAxisId={0} tick={false} />
        <RadialBar
          angleAxisId={0}
          dataKey="value"
          cornerRadius={4}
          background={{ fill: '#1f2937' }} /* gray-800 track */
          isAnimationActive={false}
        />
      </RadialBarChart>

      {/* Center overlay — score text.  Absolute so it sits inside the arc hole. */}
      <div className="absolute flex flex-col items-center leading-none pointer-events-none">
        <span className={`text-lg font-bold tabular-nums ${textClass}`}>
          {clampedScore.toFixed(0)}
        </span>
        <span className="text-[9px] uppercase tracking-wide text-gray-400 mt-0.5">health</span>
      </div>
    </div>
  )
})
