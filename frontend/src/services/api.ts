import axios from 'axios'
import type { StreamSummaryDTO } from '../types/stream.types'

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

export default api
