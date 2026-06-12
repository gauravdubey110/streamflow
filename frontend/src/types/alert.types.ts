// Mirrors AlertEventDTO from streamflow-common (backend)
export interface AlertEvent {
  alertId: string
  streamId: string
  severity: 'CRITICAL' | 'WARNING' | 'INFO'
  alertType: 'VIEWER_DROP' | 'HIGH_BUFFER_RATE' | 'BITRATE_DEGRADATION' | 'STREAM_DOWN'
  threshold: number
  actualValue: number
  message: string
  timestamp: number
}

// WebSocket push payload for alert messages (server → client)
export interface AlertFiredMessage {
  type: 'ALERT_FIRED'
  alertId: string
  streamId: string
  severity: AlertEvent['severity']
  alertType: AlertEvent['alertType']
  message: string
  ts: number
}
