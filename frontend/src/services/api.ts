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

export default api
