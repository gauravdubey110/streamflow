import { Client } from '@stomp/stompjs'
import SockJS from 'sockjs-client'

// WebSocket endpoint is read from the VITE_WS_URL env variable.
// Falls back to the API gateway default for local development.
const wsUrl = (): string => import.meta.env['VITE_WS_URL'] ?? 'http://localhost:8080/ws'

/**
 * createStompClient — factory that returns a configured STOMP Client.
 *
 * The client is NOT activated here; the caller (useWebSocket) is responsible
 * for calling client.activate() and client.deactivate().
 *
 * Configuration:
 *  - SockJS transport factory (required for Spring STOMP/SockJS endpoint)
 *  - 5-second reconnect delay
 *  - heartbeat: 10s outgoing, 10s incoming
 */
export function createStompClient(): Client {
  return new Client({
    webSocketFactory: () => new SockJS(wsUrl()) as WebSocket,
    reconnectDelay: 5000,
    heartbeatIncoming: 10000,
    heartbeatOutgoing: 10000,
  })
}
