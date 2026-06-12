import clsx from 'clsx'

interface LiveDotProps {
  connected: boolean
}

/**
 * LiveDot — a small pulsing indicator dot.
 * Green when connected to the WebSocket; gray otherwise.
 */
export function LiveDot({ connected }: LiveDotProps) {
  return (
    <span
      className={clsx(
        'inline-block h-3 w-3 rounded-full',
        connected ? 'bg-green-500 animate-pulse' : 'bg-gray-400',
      )}
      aria-label={connected ? 'Connected' : 'Disconnected'}
    />
  )
}
