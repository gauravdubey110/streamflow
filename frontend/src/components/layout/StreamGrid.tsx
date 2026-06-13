import type { StreamSummaryDTO } from '../../types/stream.types'
import { StreamCard } from '../stream/StreamCard'

interface StreamGridProps {
  /** Active streams returned by GET /api/v1/streams. */
  streams: StreamSummaryDTO[]
  /** Stream IDs that are fading out (disappeared from API response). */
  fadingIds: Set<string>
}

/**
 * StreamGrid — responsive grid of StreamCard tiles.
 *
 * Layout: 1 col on mobile, 2 col on md breakpoint, 3 col on lg+.
 *
 * Spec ref: SPEC-08 R6, R7.
 */
export function StreamGrid({ streams, fadingIds }: StreamGridProps) {
  if (streams.length === 0) {
    return (
      <div className="flex items-center justify-center py-20 text-gray-500 text-sm">
        No active streams. Waiting for data…
      </div>
    )
  }

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
      {streams.map((s) => (
        <StreamCard
          key={s.streamId}
          streamId={s.streamId}
          streamName={s.streamName}
          fading={fadingIds.has(s.streamId)}
        />
      ))}
    </div>
  )
}
