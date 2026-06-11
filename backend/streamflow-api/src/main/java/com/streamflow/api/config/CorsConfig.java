package com.streamflow.api.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * CORS configuration for REST endpoints.
 *
 * <p>SPEC-06 R5: Allows {@code http://localhost:5173} (Vite default) for all
 * REST API paths. WebSocket CORS is handled separately in {@link WebSocketConfig}
 * via {@code setAllowedOriginPatterns("*")}.
 *
 * <p>The allowed origin is configurable via {@code cors.allowed-origins} property
 * so it can be overridden in Docker/CI environments.
 */
@Configuration
public class CorsConfig implements WebMvcConfigurer {

    @Value("${cors.allowed-origins:http://localhost:5173}")
    private String allowedOrigins;

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/api/**")
                .allowedOrigins(allowedOrigins)
                .allowedMethods("GET", "POST", "PUT", "DELETE", "OPTIONS")
                .allowedHeaders("*")
                .allowCredentials(true)
                .maxAge(3600);
    }
}
