package com.streamflow.api.websocket;

/**
 * STOMP WebSocket push payload for circuit-breaker state-change notifications (SPEC-14 R4).
 *
 * <p>Broadcast to {@code /topic/streams/{streamId}/circuit-breaker} whenever a
 * {@link com.streamflow.common.dto.CbStateEventDTO} is consumed from the
 * {@code cb-events} Kafka topic.
 *
 * <p>Wire format matches the Project Plan §10
 * {@code CIRCUIT_BREAKER_STATE_CHANGE} schema:
 * <pre>{@code
 * {
 *   "type":          "CIRCUIT_BREAKER_STATE_CHANGE",
 *   "streamId":      "all",
 *   "previousState": "CLOSED",
 *   "currentState":  "OPEN",
 *   "reason":        "Failure rate 60% exceeded threshold 50%",
 *   "ts":            1717350000000
 * }
 * }</pre>
 */
public record CbWsMessage(
        /** Always {@code "CIRCUIT_BREAKER_STATE_CHANGE"} — discriminator field (SPEC-14 R4). */
        String type,

        /** Stream identifier, or {@code "all"} for the global circuit breaker. */
        String streamId,

        /** Resilience4j state before the transition, e.g. {@code "CLOSED"}. */
        String previousState,

        /** Resilience4j state after the transition, e.g. {@code "OPEN"}. */
        String currentState,

        /** Human-readable description of why the transition occurred. */
        String reason,

        /** Epoch-ms when the transition was observed. */
        long ts
) {
    /** Discriminator value used in every {@link CbWsMessage}. */
    public static final String TYPE = "CIRCUIT_BREAKER_STATE_CHANGE";
}
