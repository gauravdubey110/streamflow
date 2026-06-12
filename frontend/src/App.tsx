import { LiveDot } from './components/common/LiveDot'
import { useWebSocket } from './hooks/useWebSocket'

function App() {
  const { connected } = useWebSocket()

  return (
    <div className="min-h-screen bg-gray-950 text-white flex flex-col items-center justify-center gap-4">
      <h1 className="text-3xl font-bold tracking-tight">StreamFlow</h1>
      <div className="flex items-center gap-2">
        <LiveDot connected={connected} />
        <span className="text-sm text-gray-300">
          {connected ? 'StreamFlow \u2014 connected' : 'StreamFlow \u2014 disconnected'}
        </span>
      </div>
    </div>
  )
}

export default App
