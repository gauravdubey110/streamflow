import axios from 'axios'
import type { StreamSummaryDTO, HistoryPoint, HistoryGranularity } from '../types/stream.types'
import type { AlertEvent } from '../types/alert.types'

// Base URL is read from the VITE_API_BASE env variable (set in .env).
// Falls back to the API gateway default for local development.
const api = axios.create({
  baseURL: import.meta.env['VITE_API_BASE'] ?? 'http://localhost:8080',
  headers: {
    'Content-Type': 'application/json',
  },
})

/**
 * fetchStreams — GET /api/v1/streams
 *
 * Spec ref: SPEC-08 R1.
 */
export async function fetchStreams(): Promise<StreamSummaryDTO[]> {
  const response = await api.get<StreamSummaryDTO[]>('/api/v1/streams')
  return response.data
}

export type ChaosScenario = 'VIEWER_DROP' | 'BITRATE_SPIKE' | 'HIGH_BUFFER' | 'STREAM_DOWN'

export interface InjectChaosResponse {
  chaosId: string
  startsAt: number
}

/**
 * injectChaos — POST /api/v1/streams/{streamId}/chaos
 *
 * Spec ref: SPEC-16 R5.
 */
export async function injectChaos(
  streamId: string,
  scenario: ChaosScenario,
  durationSeconds: number,
): Promise<InjectChaosResponse> {
  const response = await api.post<InjectChaosResponse>(
    `/api/v1/streams/${streamId}/chaos`,
    { scenario, durationSeconds },
  )
  return response.data
}

/**
 * cancelChaos — DELETE /api/v1/streams/{streamId}/chaos/{chaosId}
 *
 * Spec ref: SPEC-16 R5.
 */
export async function cancelChaos(streamId: string, chaosId: string): Promise<void> {
  await api.delete(`/api/v1/streams/${streamId}/chaos/${chaosId}`)
}

/**
 * fetchStreamHistory — GET /api/v1/streams/{streamId}/history
 *
 * Returns historical metric snapshots from Cassandra in ascending timestamp order.
 * Spec ref: SPEC-19 R3.
 */
export async function fetchStreamHistory(
  streamId: string,
  from: number,
  to: number,
  granularity: HistoryGranularity,
): Promise<HistoryPoint[]> {
  const response = await api.get<HistoryPoint[]>(`/api/v1/streams/${streamId}/history`, {
    params: { from, to, granularity },
  })
  return response.data
}

/**
 * fetchStreamAlerts — GET /api/v1/streams/{streamId}/alerts
 *
 * Returns alert history from Cassandra in descending timestamp order.
 * Spec ref: SPEC-19 R3.
 */
export async function fetchStreamAlerts(
  streamId: string,
  from: number,
  to: number,
  severity?: AlertEvent['severity'],
): Promise<AlertEvent[]> {
  const response = await api.get<AlertEvent[]>(`/api/v1/streams/${streamId}/alerts`, {
    params: { from, to, ...(severity !== undefined ? { severity } : {}) },
  })
  return response.data
}

export default api
