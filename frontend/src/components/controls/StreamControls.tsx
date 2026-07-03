/**
 * StreamControls — groups CircuitBreakerIndicator + ChaosButton per stream.
 *
 * Displayed as a compact row at the bottom of each StreamCard.
 *
 * Spec ref: SPEC-16 R6.
 */
import { CircuitBreakerIndicator } from '../common/CircuitBreakerIndicator'
import { ChaosButton } from './ChaosButton'
import { useCircuitBreakerState } from '../../hooks/useCircuitBreakerState'

interface StreamControlsProps {
  streamId: string
}

export function StreamControls({ streamId }: StreamControlsProps) {
  const { state, reason } = useCircuitBreakerState(streamId)

  return (
    <div
      className="flex flex-wrap items-center justify-between gap-2 border-t border-gray-800 pt-2"
      data-testid={`stream-controls-${streamId}`}
    >
      <CircuitBreakerIndicator state={state} reason={reason} />
      <ChaosButton streamId={streamId} />
    </div>
  )
}
