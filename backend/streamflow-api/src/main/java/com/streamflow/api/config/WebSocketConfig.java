package com.streamflow.api.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

/**
 * STOMP-over-SockJS WebSocket configuration.
 *
 * <p>SPEC-06 R1:
 * <ul>
 *   <li>Endpoint: {@code /ws} (SockJS fallback enabled).</li>
 *   <li>Simple in-memory broker on destination prefix {@code /topic}.</li>
 *   <li>Application destination prefix {@code /app} (not used in MVP but
 *       reserved for future client-to-server messaging).</li>
 *   <li>Origin patterns: {@code *} — per-origin CORS is handled by
 *       {@link CorsConfig} for REST; SockJS needs its own pattern here.</li>
 * </ul>
 */
@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

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
        registry.addEndpoint("/ws")
                .setAllowedOriginPatterns("*")
                .withSockJS();
    }
}
