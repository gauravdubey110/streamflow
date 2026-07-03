/**
 * alertStore unit tests.
 *
 * Spec ref: SPEC-16 R1 — cap at 50, newest-first ordering, dedup by alertId.
 */
import { describe, it, expect, beforeEach } from 'vitest'
import { useAlertStore, MAX_ALERTS_PER_STREAM } from '../store/alertStore'
import type { AlertEvent } from '../types/alert.types'

function makeAlert(overrides?: Partial<AlertEvent>): AlertEvent {
  return {
    alertId: `alert-${Math.random().toString(36).slice(2)}`,
    streamId: 'stream-001',
    severity: 'CRITICAL',
    alertType: 'HIGH_BUFFER_RATE',
    threshold: 5.0,
    actualValue: 8.3,
    message: 'Buffer rate 8.3% exceeds threshold 5.0%',
    timestamp: Date.now(),
    ...overrides,
  }
}

function getAlerts(streamId: string): AlertEvent[] {
  // Record<string, AlertEvent[]> is safe after we've added items;
  // fall back to [] to satisfy strict null checks.
  return useAlertStore.getState().alerts[streamId] ?? []
}

// Reset store state between tests.
beforeEach(() => {
  useAlertStore.setState({ alerts: {} })
})

describe('alertStore', () => {
  it('adds an alert to the front of the list (newest-first)', () => {
    const first = makeAlert({ alertId: 'a1', timestamp: 1000 })
    const second = makeAlert({ alertId: 'a2', timestamp: 2000 })

    useAlertStore.getState().addAlert(first)
    useAlertStore.getState().addAlert(second)

    const alerts = getAlerts('stream-001')
    expect(alerts[0]?.alertId).toBe('a2')
    expect(alerts[1]?.alertId).toBe('a1')
  })

  it(`caps list at ${MAX_ALERTS_PER_STREAM} entries`, () => {
    for (let i = 0; i < MAX_ALERTS_PER_STREAM + 10; i++) {
      useAlertStore.getState().addAlert(makeAlert({ alertId: `alert-${i}` }))
    }
    const alerts = getAlerts('stream-001')
    expect(alerts.length).toBe(MAX_ALERTS_PER_STREAM)
  })

  it('deduplicates alerts by alertId', () => {
    const alert = makeAlert({ alertId: 'dup-1' })
    useAlertStore.getState().addAlert(alert)
    useAlertStore.getState().addAlert(alert) // second call with same alertId

    const alerts = getAlerts('stream-001')
    expect(alerts.length).toBe(1)
  })

  it('clearAlerts empties the list for a stream', () => {
    useAlertStore.getState().addAlert(makeAlert())
    useAlertStore.getState().clearAlerts('stream-001')

    const alerts = getAlerts('stream-001')
    expect(alerts.length).toBe(0)
  })

  it('stores alerts per stream independently', () => {
    useAlertStore.getState().addAlert(makeAlert({ streamId: 'stream-001' }))
    useAlertStore.getState().addAlert(makeAlert({ streamId: 'stream-002' }))

    expect(getAlerts('stream-001').length).toBe(1)
    expect(getAlerts('stream-002').length).toBe(1)
  })
})
