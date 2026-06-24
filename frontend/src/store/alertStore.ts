/**
 * alertStore — Zustand store for the per-stream alert feed.
 *
 * Alerts are stored newest-first per streamId, capped at MAX_ALERTS_PER_STREAM.
 * The store is persisted to sessionStorage so a page refresh during a demo
 * retains the alert history (Spec ref: SPEC-16 Design Notes).
 *
 * Spec ref: SPEC-16 R1.
 */
import { create } from 'zustand'
import { persist, createJSONStorage } from 'zustand/middleware'
import type { AlertEvent } from '../types/alert.types'

export const MAX_ALERTS_PER_STREAM = 50

interface AlertStoreState {
  /** Map of streamId → alerts array, newest first. */
  alerts: Record<string, AlertEvent[]>
  /** Append an alert to the front of the list for its stream; cap at 50. */
  addAlert: (alert: AlertEvent) => void
  /** Clear all alerts for a given streamId. */
  clearAlerts: (streamId: string) => void
}

export const useAlertStore = create<AlertStoreState>()(
  persist(
    (set) => ({
      alerts: {},

      addAlert: (alert) =>
        set((state) => {
          const existing = state.alerts[alert.streamId] ?? []
          // Deduplicate by alertId to guard against duplicate messages.
          if (existing.some((a) => a.alertId === alert.alertId)) {
            return state
          }
          const updated = [alert, ...existing].slice(0, MAX_ALERTS_PER_STREAM)
          return {
            alerts: {
              ...state.alerts,
              [alert.streamId]: updated,
            },
          }
        }),

      clearAlerts: (streamId) =>
        set((state) => ({
          alerts: {
            ...state.alerts,
            [streamId]: [],
          },
        })),
    }),
    {
      name: 'streamflow-alerts',
      storage: createJSONStorage(() => sessionStorage),
    },
  ),
)
