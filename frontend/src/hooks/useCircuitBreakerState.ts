/**
 * useCircuitBreakerState — subscribes to `/topic/streams/{streamId}/circuit-breaker`
 * and returns the current CB state + last reason string.
 *
 * Defaults to CLOSED before the first message arrives.
 *
 * Spec ref: SPEC-16 R3.
 */
import { useEffect, useState } from 'react'
import { useWebSocket } from './useWebSocket'
import type { CircuitBreakerStateChangeMessage } from '../types/stream.types'

export type CBState = 'CLOSED' | 'OPEN' | 'HALF_OPEN'

export interface UseCircuitBreakerStateResult {
  state: CBState
  reason: string | null
}

export function useCircuitBreakerState(streamId: string): UseCircuitBreakerStateResult {
  const { subscribe, unsubscribe } = useWebSocket()
  const [cbState, setCbState] = useState<CBState>('CLOSED')
  const [reason, setReason] = useState<string | null>(null)

  useEffect(() => {
    const destination = `/topic/streams/${streamId}/circuit-breaker`

    const subId = subscribe(destination, (message) => {
      try {
        const msg = JSON.parse(message.body) as CircuitBreakerStateChangeMessage
        setCbState(msg.currentState)
        setReason(msg.reason ?? null)
      } catch (err) {
        console.error('[useCircuitBreakerState] Failed to parse CB message:', err)
      }
    })

    return () => {
      unsubscribe(subId)
      // Reset to default on stream change so stale state is not shown.
      setCbState('CLOSED')
      setReason(null)
    }
  }, [streamId, subscribe, unsubscribe])

  return { state: cbState, reason }
}
