package com.streamflow.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for the StreamFlow API Gateway service.
 *
 * <p>Responsibilities (implemented in later specs):
 * <ul>
 *   <li>SPEC-06: STOMP/SockJS WebSocket config + metrics push to /topic/streams/{id}/metrics
 *   <li>SPEC-06: REST GET /api/v1/streams/** endpoints
 *   <li>SPEC-14: Alert + circuit-breaker WebSocket push topics
 *   <li>SPEC-18: Historical replay — GET /api/v1/streams/{id}/history
 *   <li>SPEC-20: Prometheus + Actuator observability
 * </ul>
 */
@SpringBootApplication
public class StreamApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(StreamApiApplication.class, args);
    }
}
