/**
 * CircuitBreakerIndicator — pill showing STOMP circuit breaker state.
 *
 * States:
 *   CLOSED    → green pill  (normal operation)
 *   HALF_OPEN → yellow pill (probing)
 *   OPEN      → red pill    (tripped; requests blocked)
 *
 * Renders a tooltip (via title attribute) with the reason string when available.
 *
 * Spec ref: SPEC-16 R4.
 */
import clsx from 'clsx'
import type { CBState } from '../../hooks/useCircuitBreakerState'

interface CircuitBreakerIndicatorProps {
  state: CBState
  reason?: string | null
}

const STATE_STYLES: Record<CBState, string> = {
  CLOSED: 'bg-green-900/60 text-green-300 border border-green-700',
  HALF_OPEN: 'bg-amber-900/60 text-amber-300 border border-amber-700',
  OPEN: 'bg-red-900/60 text-red-300 border border-red-700',
}

const STATE_LABELS: Record<CBState, string> = {
  CLOSED: 'CB: CLOSED',
  HALF_OPEN: 'CB: HALF OPEN',
  OPEN: 'CB: OPEN',
}

export function CircuitBreakerIndicator({ state, reason }: CircuitBreakerIndicatorProps) {
  return (
    <span
      className={clsx(
        'inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide cursor-default select-none',
        STATE_STYLES[state],
      )}
      title={reason ?? STATE_LABELS[state]}
      aria-label={`Circuit breaker: ${state}${reason ? ` — ${reason}` : ''}`}
      data-testid="cb-indicator"
      data-state={state}
    >
      <span
        className={clsx('mr-1 h-1.5 w-1.5 rounded-full', {
          'bg-green-400': state === 'CLOSED',
          'bg-amber-400': state === 'HALF_OPEN',
          'bg-red-400': state === 'OPEN',
        })}
        aria-hidden="true"
      />
      {STATE_LABELS[state]}
    </span>
  )
}
