package com.streamflow.processor.circuitbreaker;

import com.streamflow.processor.metrics.ProcessorMetrics;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Bridges the Resilience4j {@code alertProcessor} circuit breaker with Redis and
 * Spring's event bus.
 *
 * <p>SPEC-12 R5 — on every state transition the current state is written to Redis:
 * <pre>SET cb_state:alert_processor &lt;STATE&gt; EX 300</pre>
 *
 * <p>SPEC-12 R6 — a {@link CircuitBreakerStateEvent} is published on
 * {@link ApplicationEventPublisher} so SPEC-14 can broadcast the change to WebSocket
 * clients.
 *
 * <p>Design note (SPEC-12 §4 / Open Question Q1): Resilience4j does not natively
 * support a truly distributed circuit breaker with consensus. The Redis write is a
 * "last-writer-wins" view — useful for dashboarding but not a distributed consensus.
 * Each processor instance runs its own Resilience4j state machine; they all write to
 * the same Redis key, so the dashboard shows whichever instance last transitioned.
 * This trade-off is intentional and documented in the project README.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AlertProcessorCircuitBreaker {

    /** Name of the Resilience4j circuit breaker instance (matches application.properties). */
    public static final String CB_NAME = "alertProcessor";

    /**
     * Redis key where the current CB state is persisted.
     *
     * <p>Project Plan §7 shows {@code cb_state:alert_processor:{streamId}} but because
     * Resilience4j uses a single CB per name (not per stream), SPEC-12 R5 specifies a
     * single key {@code cb_state:alert_processor}. See deviation note in spec.
     */
    public static final String CB_STATE_KEY = "cb_state:alert_processor";

    /** TTL for the Redis state key — 5 minutes (SPEC-12 R5). */
    private static final Duration CB_STATE_TTL = Duration.ofSeconds(300);

    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final RedisTemplate<String, String> redisTemplate;
    private final ApplicationEventPublisher eventPublisher;
    private final ProcessorMetrics processorMetrics;

    /**
     * Registers the state-transition listener after the bean is initialised.
     *
     * <p>The listener is registered once (idempotent — Resilience4j deduplicates
     * by reference equality). It writes Redis and publishes a Spring event on
     * every transition: CLOSED → OPEN, OPEN → HALF_OPEN, HALF_OPEN → CLOSED,
     * and HALF_OPEN → OPEN.
     */
    @PostConstruct
    public void registerStateChangeListener() {
        CircuitBreaker cb = circuitBreakerRegistry.circuitBreaker(CB_NAME);

        cb.getEventPublisher().onStateTransition(event -> {
            String previousState = event.getStateTransition().getFromState().name();
            String currentState  = event.getStateTransition().getToState().name();
            long   now           = System.currentTimeMillis();
            String reason        = "Circuit breaker transitioned from " + previousState
                                   + " to " + currentState;

            log.info("SPEC-12: CB '{}' state transition: {} → {} at {}ms",
                    CB_NAME, previousState, currentState, now);

            // SPEC-12 R5: persist state to Redis
            persistStateToRedis(currentState);

            // SPEC-20 R3: update CB state gauge
            processorMetrics.updateCbState(currentState);

            // SPEC-12 R6: publish Spring event for SPEC-14 WebSocket broadcast
            eventPublisher.publishEvent(
                    new CircuitBreakerStateEvent(this, previousState, currentState, reason, now));
        });

        // Write initial state on startup so the dashboard always has a value
        String initialState = cb.getState().name();
        persistStateToRedis(initialState);
        // SPEC-20 R3: initialise the gauge with the current state
        processorMetrics.updateCbState(initialState);
        log.info("SPEC-12: AlertProcessorCircuitBreaker listener registered; initial state={}",
                cb.getState().name());
    }

    /**
     * Returns the current CB state as a string, reading from the local Resilience4j
     * registry (not Redis, to avoid a network round-trip on every snapshot cycle).
     *
     * @return state string: {@code "CLOSED"}, {@code "OPEN"}, or {@code "HALF_OPEN"}
     */
    public String getCurrentState() {
        return circuitBreakerRegistry.circuitBreaker(CB_NAME).getState().name();
    }

    // ── internal ──────────────────────────────────────────────────────────────

    /**
     * Writes {@code SET cb_state:alert_processor <state> EX 300} to Redis.
     *
     * @param state the Resilience4j state name (e.g. {@code "OPEN"})
     */
    private void persistStateToRedis(String state) {
        try {
            redisTemplate.opsForValue().set(CB_STATE_KEY, state, CB_STATE_TTL);
            log.debug("SPEC-12: persisted CB state to Redis: key={} value={}", CB_STATE_KEY, state);
        } catch (Exception e) {
            log.error("SPEC-12: failed to persist CB state to Redis: {}", e.getMessage(), e);
        }
    }
}
