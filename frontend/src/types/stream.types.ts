/**
 * Granularity for historical queries.
 * Spec ref: SPEC-19 R2.
 */
export type HistoryGranularity = 'MINUTE' | 'HOUR'

/**
 * One data point returned by GET /api/v1/streams/{id}/history.
 * Mirrors StreamMetricSnapshotDTO (subset of fields used for replay chart).
 * Spec ref: SPEC-19 R3.
 */
export interface HistoryPoint {
  streamId: string
  snapshotTs: number
  liveViewerCount: number
  bufferRatePct: number
  p95LatencyMs: number
  healthScore: number
  qualityDistribution: Record<string, number>
}

// Mirrors StreamMetricSnapshotDTO from streamflow-common (backend)
export interface StreamMetricSnapshot {
  streamId: string
  streamName: string | null
  liveViewerCount: number
  viewerDelta: number
  bufferRatePct: number
  p95LatencyMs: number
  qualityDistribution: Record<string, number>
  healthScore: number
  circuitBreakerState: 'CLOSED' | 'OPEN' | 'HALF_OPEN'
  activeAlerts: number
  snapshotTs: number
}

// Mirrors StreamSummaryDTO from streamflow-api (REST response for GET /api/v1/streams)
export interface StreamSummaryDTO {
  streamId: string
  streamName: string | null
  liveViewerCount: number
  healthScore: number
  activeAlerts: number
  circuitBreakerState: 'CLOSED' | 'OPEN' | 'HALF_OPEN'
}

// WebSocket push payload shapes (server → client)

export interface MetricsUpdateMessage {
  type: 'METRICS_UPDATE'
  streamId: string
  liveViewerCount: number
  viewerDelta: number
  bufferRatePct: number
  p95LatencyMs: number
  qualityDistribution: Record<string, number>
  healthScore: number
  ts: number
}

export interface CircuitBreakerStateChangeMessage {
  type: 'CIRCUIT_BREAKER_STATE_CHANGE'
  streamId: string
  previousState: 'CLOSED' | 'OPEN' | 'HALF_OPEN'
  currentState: 'CLOSED' | 'OPEN' | 'HALF_OPEN'
  reason: string
  ts: number
}
