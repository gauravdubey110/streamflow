package com.streamflow.common.dto;

/**
 * Kafka message payload for the {@code cb-events} topic (SPEC-14 R2/R3).
 *
 * <p>Published by the stream-processor whenever the Resilience4j {@code alertProcessor} circuit
 * breaker transitions between states, and consumed by the API gateway to broadcast the change to
 * WebSocket clients on {@code /topic/streams/{streamId}/circuit-breaker}.
 *
 * <p>Because the current CB is a single named instance (not per-stream), {@code streamId} is set to
 * {@code "all"} by the publisher and the API gateway broadcasts to {@code
 * /topic/streams/all/circuit-breaker}.
 *
 * <p>Wire format matches the Project Plan §10 {@code CIRCUIT_BREAKER_STATE_CHANGE} schema (minus
 * the {@code type} field, which is added by the API-side WebSocket envelope).
 */
public record CbStateEventDTO(
    /** Stream identifier, or {@code "all"} for the global circuit breaker. */
    String streamId,

    /** Resilience4j state before the transition, e.g. {@code "CLOSED"}. */
    String previousState,

    /** Resilience4j state after the transition, e.g. {@code "OPEN"}. */
    String currentState,

    /** Human-readable description of why the transition occurred. */
    String reason,

    /** Epoch-ms when the transition was observed in the processor. */
    long ts) {}
