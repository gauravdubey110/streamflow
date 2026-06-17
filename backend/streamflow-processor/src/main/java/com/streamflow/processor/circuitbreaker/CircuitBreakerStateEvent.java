package com.streamflow.processor.circuitbreaker;

import org.springframework.context.ApplicationEvent;

/**
 * Spring application event published when the {@code alertProcessor} Resilience4j
 * circuit breaker transitions between states.
 *
 * <p>SPEC-12 R6: published by {@link AlertProcessorCircuitBreaker} on every state
 * transition so that SPEC-14 ({@code AlertPushConsumer}) can broadcast the change
 * to WebSocket clients on {@code /topic/streams/{streamId}/circuit-breaker}.
 *
 * <p>Because this is a single global circuit breaker (one per named Resilience4j
 * instance), {@code streamId} is set to {@code "all"} — downstream consumers may
 * choose to broadcast to all stream topics.
 */
public class CircuitBreakerStateEvent extends ApplicationEvent {

    private final String previousState;
    private final String currentState;
    private final String reason;
    private final long timestamp;

    /**
     * @param source        the bean that published this event
     * @param previousState the CB state before the transition (e.g. {@code "CLOSED"})
     * @param currentState  the CB state after the transition (e.g. {@code "OPEN"})
     * @param reason        human-readable reason for the transition
     * @param timestamp     epoch-ms when the transition occurred
     */
    public CircuitBreakerStateEvent(
            Object source,
            String previousState,
            String currentState,
            String reason,
            long timestamp) {
        super(source);
        this.previousState = previousState;
        this.currentState  = currentState;
        this.reason        = reason;
        this.timestamp     = timestamp;
    }

    /** The state the circuit breaker was in before this transition. */
    public String getPreviousState() {
        return previousState;
    }

    /** The state the circuit breaker moved to. */
    public String getCurrentState() {
        return currentState;
    }

    /** Human-readable description of why the transition occurred. */
    public String getReason() {
        return reason;
    }

    /**
     * Epoch-ms timestamp when the CB transition occurred.
     *
     * <p>Note: named {@code occurredAt} (not {@code timestamp}) because
     * {@code ApplicationEvent#getTimestamp()} is a final method in the
     * Spring parent class and cannot be overridden.
     */
    public long getOccurredAt() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "CircuitBreakerStateEvent{previousState='" + previousState
                + "', currentState='" + currentState
                + "', reason='" + reason
                + "', occurredAt=" + timestamp + '}';
    }
}
