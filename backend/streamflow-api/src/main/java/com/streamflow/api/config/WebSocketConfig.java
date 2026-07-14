package com.streamflow.api.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketTransportRegistration;

/**
 * STOMP-over-SockJS WebSocket configuration.
 *
 * <p>SPEC-06 R1:
 *
 * <ul>
 *   <li>Endpoint: {@code /ws} (SockJS fallback enabled).
 *   <li>Simple in-memory broker on destination prefix {@code /topic}.
 *   <li>Application destination prefix {@code /app} (not used in MVP but reserved for future
 *       client-to-server messaging).
 *   <li>Origin patterns: {@code *} — per-origin CORS is handled by {@link CorsConfig} for REST;
 *       SockJS needs its own pattern here.
 * </ul>
 *
 * <p>SPEC-14 R5: message size limit raised to 64 KB to accommodate alert and circuit-breaker
 * payloads without back-pressure drops on slow clients.
 */
@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

  /** 64 KB message size limit per SPEC-14 R5. */
  private static final int MESSAGE_SIZE_LIMIT = 64 * 1024;

  @Override
  public void configureMessageBroker(MessageBrokerRegistry registry) {
    // Simple in-memory broker for /topic/** destinations (SPEC-06 R1)
    registry.enableSimpleBroker("/topic");
    // Prefix for client-to-server STOMP SEND frames
    registry.setApplicationDestinationPrefixes("/app");
  }

  @Override
  public void registerStompEndpoints(StompEndpointRegistry registry) {
    // SPEC-06 R1: STOMP endpoint at /ws; SockJS fallback; allow all origins
    registry.addEndpoint("/ws").setAllowedOriginPatterns("*").withSockJS();
  }

  /**
   * SPEC-14 R5: increase the send buffer / message size limits to 64 KB so that alert and
   * circuit-breaker payloads are never silently dropped on slow clients.
   */
  @Override
  public void configureWebSocketTransport(WebSocketTransportRegistration registration) {
    registration.setMessageSizeLimit(MESSAGE_SIZE_LIMIT);
    registration.setSendBufferSizeLimit(MESSAGE_SIZE_LIMIT);
  }
}
