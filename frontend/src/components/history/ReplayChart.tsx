/**
 * ReplayChart — Recharts AreaChart for historical metric replay.
 *
 * Renders:
 * - Stacked area for viewer count (left Y axis, fill gradient).
 * - Line for buffer rate % (right Y axis).
 * - Scatter dots for alert events, coloured by severity (CRITICAL=red, WARNING=amber, INFO=blue).
 *   Clicking a dot opens a tooltip with the alert message.
 *
 * Performance: isAnimationActive={false} for datasets > 200 points (NFR1).
 *
 * Spec ref: SPEC-19 R4, NFR1.
 */
import { useState, useCallback } from 'react'
import {
  ComposedChart,
  Area,
  Line,
  Scatter,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from 'recharts'
import type { HistoryPoint } from '../../types/stream.types'
import type { AlertEvent } from '../../types/alert.types'

interface ReplayChartProps {
  data: HistoryPoint[]
  alerts: AlertEvent[]
}

/** Map alert events into (x, y) scatter points aligned to chart x domain. */
interface AlertScatterPoint {
  ts: number
  bufferRatePct: number
  alertId: string
  severity: AlertEvent['severity']
  message: string
  alertType: string
}

function severityColor(severity: AlertEvent['severity']): string {
  switch (severity) {
    case 'CRITICAL':
      return '#ef4444'
    case 'WARNING':
      return '#f59e0b'
    case 'INFO':
      return '#3b82f6'
    default:
      return '#6b7280'
  }
}

function formatTick(ts: number): string {
  return new Date(ts).toLocaleTimeString('en-GB', {
    hour: '2-digit',
    minute: '2-digit',
  })
}

/** Custom dot for alert scatter points. */
function AlertDot(props: Record<string, unknown>) {
  const { cx, cy, payload } = props as {
    cx: number
    cy: number
    payload: AlertScatterPoint
  }
  if (cx === undefined || cy === undefined) return null
  return (
    <circle
      cx={cx}
      cy={cy}
      r={5}
      fill={severityColor(payload.severity)}
      stroke="#111827"
      strokeWidth={1.5}
      style={{ cursor: 'pointer' }}
    />
  )
}

/** Custom tooltip: shows metrics for chart hover + alert detail when a dot is active. */
function CustomTooltip(props: Record<string, unknown>) {
  const { active, payload, label } = props as {
    active: boolean
    payload: Array<{ name: string; value: number; payload: Record<string, unknown> }>
    label: number
  }

  if (!active || !payload || payload.length === 0) return null

  const ts = typeof label === 'number' ? label : 0

  return (
    <div
      className="rounded border border-gray-700 bg-gray-900 px-3 py-2 text-xs shadow-lg"
      data-testid="replay-tooltip"
    >
      <p className="mb-1 font-semibold text-gray-300">
        {ts > 0
          ? new Date(ts).toLocaleString('en-GB', {
              hour: '2-digit',
              minute: '2-digit',
              second: '2-digit',
              day: '2-digit',
              month: 'short',
            })
          : ''}
      </p>
      {payload.map((entry, idx) => {
        // Alert scatter entries have alertId in payload.
        const alertPayload = entry.payload as unknown as AlertScatterPoint
        if (alertPayload.alertId !== undefined) {
          return (
            <div key={idx} className="mt-1 border-t border-gray-700 pt-1">
              <span
                className="font-semibold"
                style={{ color: severityColor(alertPayload.severity) }}
              >
                {alertPayload.severity}
              </span>{' '}
              <span className="text-gray-300">{alertPayload.alertType.replace(/_/g, ' ')}</span>
              <p className="mt-0.5 text-gray-400">{alertPayload.message}</p>
            </div>
          )
        }
        return (
          <p key={idx} style={{ color: entry.name === 'Buffer Rate %' ? '#f59e0b' : '#60a5fa' }}>
            {entry.name}: <span className="font-mono">{entry.value?.toLocaleString('en-US')}</span>
          </p>
        )
      })}
    </div>
  )
}

const fmt = new Intl.NumberFormat('en-US')

export function ReplayChart({ data, alerts }: ReplayChartProps) {
  const [activeAlertId, setActiveAlertId] = useState<string | null>(null)

  // Build scatter points from alerts, y-position at their buffer rate if known,
  // otherwise at 0 so they appear on the x-axis baseline.
  const alertPoints: AlertScatterPoint[] = alerts.map((a) => ({
    ts: a.timestamp,
    bufferRatePct: 0,
    alertId: a.alertId,
    severity: a.severity,
    message: a.message,
    alertType: a.alertType,
  }))

  const handleAlertDotClick = useCallback((point: AlertScatterPoint) => {
    setActiveAlertId((prev) => (prev === point.alertId ? null : point.alertId))
  }, [])

  const isLargeDataset = data.length > 200

  if (data.length === 0) {
    return (
      <div
        className="flex h-48 items-center justify-center text-sm text-gray-500"
        data-testid="replay-chart-empty"
        role="status"
        aria-live="polite"
      >
        No data in this range
      </div>
    )
  }

  const activeAlert = alerts.find((a) => a.alertId === activeAlertId)

  return (
    <div className="flex flex-col gap-2" data-testid="replay-chart">
      {activeAlert !== undefined && (
        <div
          className="rounded border border-amber-700/50 bg-amber-900/20 px-3 py-2 text-xs"
          role="alert"
          aria-live="polite"
          data-testid="replay-active-alert"
        >
          <span
            className="font-semibold"
            style={{ color: severityColor(activeAlert.severity) }}
          >
            {activeAlert.severity}
          </span>{' '}
          — {activeAlert.message}
        </div>
      )}
      <ResponsiveContainer width="100%" height={260}>
        <ComposedChart data={data} margin={{ top: 4, right: 16, left: 0, bottom: 0 }}>
          <defs>
            <linearGradient id="viewerGradient" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.4} />
              <stop offset="95%" stopColor="#3b82f6" stopOpacity={0.02} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis
            dataKey="snapshotTs"
            type="number"
            domain={['auto', 'auto']}
            scale="time"
            tickFormatter={formatTick}
            tick={{ fill: '#9ca3af', fontSize: 10 }}
            axisLine={{ stroke: '#374151' }}
            tickLine={false}
          />
          <YAxis
            yAxisId="left"
            tickFormatter={(v: number) => fmt.format(v)}
            tick={{ fill: '#9ca3af', fontSize: 10 }}
            axisLine={{ stroke: '#374151' }}
            tickLine={false}
            width={60}
          />
          <YAxis
            yAxisId="right"
            orientation="right"
            tickFormatter={(v: number) => `${v.toFixed(1)}%`}
            tick={{ fill: '#9ca3af', fontSize: 10 }}
            axisLine={{ stroke: '#374151' }}
            tickLine={false}
            width={40}
          />
          <Tooltip content={<CustomTooltip />} />
          <Legend wrapperStyle={{ fontSize: '11px', color: '#9ca3af' }} />
          <Area
            yAxisId="left"
            type="monotone"
            dataKey="liveViewerCount"
            name="Viewers"
            stroke="#3b82f6"
            strokeWidth={1.5}
            fill="url(#viewerGradient)"
            dot={false}
            isAnimationActive={!isLargeDataset}
          />
          <Line
            yAxisId="right"
            type="monotone"
            dataKey="bufferRatePct"
            name="Buffer Rate %"
            stroke="#f59e0b"
            strokeWidth={1.5}
            dot={false}
            isAnimationActive={!isLargeDataset}
          />
          {/* Alert scatter dots */}
          {alertPoints.length > 0 && (
            <Scatter
              yAxisId="left"
              data={alertPoints}
              dataKey="ts"
              name="Alerts"
              shape={<AlertDot />}
              onClick={(point) => handleAlertDotClick(point as unknown as AlertScatterPoint)}
              isAnimationActive={false}
            />
          )}
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  )
}
