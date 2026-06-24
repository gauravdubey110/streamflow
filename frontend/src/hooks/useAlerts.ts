/**
 * useAlerts — subscribes to `/topic/streams/{streamId}/alerts` and
 * appends each incoming alert to the Zustand alertStore.
 *
 * Returns the current alert list for the given stream (newest first).
 *
 * Spec ref: SPEC-16 R1.
 */
import { useEffect } from 'react'
import { useWebSocket } from './useWebSocket'
import { useAlertStore } from '../store/alertStore'
import type { AlertFiredMessage } from '../types/alert.types'
import type { AlertEvent } from '../types/alert.types'

export function useAlerts(streamId: string): AlertEvent[] {
  const { subscribe, unsubscribe } = useWebSocket()
  const addAlert = useAlertStore((s) => s.addAlert)
  const alerts = useAlertStore((s) => s.alerts[streamId] ?? [])

  useEffect(() => {
    const destination = `/topic/streams/${streamId}/alerts`

    const subId = subscribe(destination, (message) => {
      try {
        const msg = JSON.parse(message.body) as AlertFiredMessage

        const alert: AlertEvent = {
          alertId: msg.alertId,
          streamId: msg.streamId,
          severity: msg.severity,
          alertType: msg.alertType,
          // threshold / actualValue are not in the WS push payload; default to 0
          threshold: 0,
          actualValue: 0,
          message: msg.message,
          timestamp: msg.ts,
        }

        addAlert(alert)
      } catch (err) {
        console.error('[useAlerts] Failed to parse alert message:', err)
      }
    })

    return () => {
      unsubscribe(subId)
    }
  }, [streamId, subscribe, unsubscribe, addAlert])

  return alerts
}
