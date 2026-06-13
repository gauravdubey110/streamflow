import { memo } from 'react'
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
} from 'recharts'
import type { ValueType, NameType } from 'recharts/types/component/DefaultTooltipContent'
import type { ChartPoint } from '../../hooks/useStreamMetrics'

interface ViewerCountChartProps {
  /** Up to 60 data points from the ring buffer in useStreamMetrics. */
  history: ChartPoint[]
}

/**
 * ViewerCountChart — a Recharts LineChart that renders the last ≤60 seconds of
 * live viewer counts.  Wrapped in React.memo so it only re-renders when the
 * `history` array reference changes (i.e., on each new WebSocket push).
 *
 * Spec ref: SPEC-08 R4.
 */
export const ViewerCountChart = memo(function ViewerCountChart({
  history,
}: ViewerCountChartProps) {
  return (
    <ResponsiveContainer width="100%" height={120}>
      <LineChart data={history} margin={{ top: 4, right: 8, bottom: 0, left: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
        <XAxis
          dataKey="time"
          tick={{ fill: '#9ca3af', fontSize: 10 }}
          interval="preserveStartEnd"
          minTickGap={40}
        />
        <YAxis
          tick={{ fill: '#9ca3af', fontSize: 10 }}
          width={48}
          tickFormatter={(v: number) =>
            v >= 1_000_000
              ? `${(v / 1_000_000).toFixed(1)}M`
              : v >= 1_000
                ? `${(v / 1_000).toFixed(0)}K`
                : String(v)
          }
        />
        <Tooltip
          contentStyle={{
            backgroundColor: '#111827',
            border: '1px solid #374151',
            borderRadius: 4,
            color: '#f9fafb',
            fontSize: 12,
          }}
          formatter={(value: ValueType | undefined): [string, NameType] => [
            typeof value === 'number' ? new Intl.NumberFormat().format(value) : String(value ?? ''),
            'Viewers',
          ]}
          labelFormatter={(label) => `Time: ${String(label ?? '')}`}
        />
        <Line
          type="monotone"
          dataKey="liveViewerCount"
          stroke="#3b82f6"
          strokeWidth={2}
          dot={false}
          isAnimationActive={true}
          animationDuration={400}
          animationEasing="ease-out"
        />
      </LineChart>
    </ResponsiveContainer>
  )
})
