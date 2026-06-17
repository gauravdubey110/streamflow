package com.streamflow.processor.circuitbreaker;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit test for {@link AlertProcessorCircuitBreaker}.
 *
 * <p>SPEC-12 Test Plan — Unit:
 * <ul>
 *   <li>Programmatically drive the CB through CLOSED → OPEN by recording
 *       failures against the Resilience4j registry.</li>
 *   <li>Assert that the listener writes the new state to Redis (SPEC-12 R5).</li>
 *   <li>Assert that a {@link CircuitBreakerStateEvent} is published on the
 *       Spring {@link ApplicationEventPublisher} (SPEC-12 R6).</li>
 *   <li>Assert that {@link AlertProcessorCircuitBreaker#getCurrentState()} returns
 *       the current state string (SPEC-12 R7 — used by SnapshotPublisher).</li>
 * </ul>
 *
 * <p>No infrastructure required — all Redis and Spring interactions are mocked.
 */
@ExtendWith(MockitoExtension.class)
class CircuitBreakerStateListenerTest {

    @Mock
    private RedisTemplate<String, String> redisTemplate;

    @Mock
    @SuppressWarnings("rawtypes")
    private ValueOperations valueOperations;

    @Mock
    private ApplicationEventPublisher eventPublisher;

    private CircuitBreakerRegistry registry;
    private AlertProcessorCircuitBreaker cbComponent;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);

        // Build a CB with low thresholds to make testing fast:
        // sliding window = 4 calls, minimum = 2, failure rate = 50% → opens on 2/4 failures
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(4)
                .minimumNumberOfCalls(2)
                .failureRateThreshold(50.0f)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(2)
                .build();

        registry = CircuitBreakerRegistry.of(config);

        cbComponent = new AlertProcessorCircuitBreaker(registry, redisTemplate, eventPublisher);
        cbComponent.registerStateChangeListener();
    }

    // ── Redis persistence on transition ──────────────────────────────────────

    /**
     * SPEC-12 R5: drive CLOSED → OPEN; assert Redis key is written with value "OPEN".
     */
    @Test
    @SuppressWarnings("unchecked")
    void stateTransitionToOpen_writesRedisKey() {
        CircuitBreaker cb = registry.circuitBreaker(AlertProcessorCircuitBreaker.CB_NAME);

        // Record enough failures to trip the CB
        forceOpen(cb);

        assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.OPEN);

        // Verify Redis SET was called with OPEN state
        ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
        verify(valueOperations, atLeastOnce()).set(
                eq(AlertProcessorCircuitBreaker.CB_STATE_KEY),
                valueCaptor.capture(),
                any(Duration.class)
        );

        List<String> writtenValues = valueCaptor.getAllValues();
        assertThat(writtenValues)
                .as("Redis should have been written with OPEN state")
                .contains("OPEN");
    }

    /**
     * SPEC-12 R5: initial state (CLOSED) is written to Redis on startup.
     */
    @Test
    @SuppressWarnings("unchecked")
    void initialState_writesClosedToRedis() {
        // The @PostConstruct already ran in setUp; verify at least one CLOSED write
        ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
        verify(valueOperations, atLeastOnce()).set(
                eq(AlertProcessorCircuitBreaker.CB_STATE_KEY),
                valueCaptor.capture(),
                any(Duration.class)
        );

        assertThat(valueCaptor.getAllValues())
                .as("Redis should have been written with initial CLOSED state on startup")
                .contains("CLOSED");
    }

    // ── Spring event published on transition ─────────────────────────────────

    /**
     * SPEC-12 R6: driving CLOSED → OPEN must publish a {@link CircuitBreakerStateEvent}.
     */
    @Test
    void stateTransitionToOpen_publishesSpringEvent() {
        CircuitBreaker cb = registry.circuitBreaker(AlertProcessorCircuitBreaker.CB_NAME);
        forceOpen(cb);

        ArgumentCaptor<CircuitBreakerStateEvent> eventCaptor =
                ArgumentCaptor.forClass(CircuitBreakerStateEvent.class);
        verify(eventPublisher, atLeastOnce()).publishEvent(eventCaptor.capture());

        CircuitBreakerStateEvent event = eventCaptor.getAllValues().stream()
                .filter(e -> "OPEN".equals(e.getCurrentState()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No OPEN state event published"));

        assertThat(event.getPreviousState()).isEqualTo("CLOSED");
        assertThat(event.getCurrentState()).isEqualTo("OPEN");
        assertThat(event.getReason()).contains("CLOSED").contains("OPEN");
        assertThat(event.getOccurredAt()).isGreaterThan(0L);
    }

    // ── getCurrentState() reflects local CB ──────────────────────────────────

    /**
     * SPEC-12 R7: {@link AlertProcessorCircuitBreaker#getCurrentState()} returns
     * the correct state string for use by {@link com.streamflow.processor.snapshot.SnapshotPublisher}.
     */
    @Test
    void getCurrentState_returnsClosed_initially() {
        assertThat(cbComponent.getCurrentState())
                .as("Initial CB state should be CLOSED")
                .isEqualTo("CLOSED");
    }

    @Test
    void getCurrentState_returnsOpen_afterFailures() {
        CircuitBreaker cb = registry.circuitBreaker(AlertProcessorCircuitBreaker.CB_NAME);
        forceOpen(cb);

        assertThat(cbComponent.getCurrentState())
                .as("CB state should be OPEN after recording failures")
                .isEqualTo("OPEN");
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Records enough failures on the given CB to force it to OPEN state.
     *
     * <p>The test CB config: sliding window = 4, minimum calls = 2, failure rate = 50%.
     * Recording 2 failures out of 2 calls (100%) exceeds the 50% threshold.
     */
    private static void forceOpen(CircuitBreaker cb) {
        // Record calls as failures to trip the CB
        for (int i = 0; i < 4; i++) {
            cb.onError(0, java.util.concurrent.TimeUnit.NANOSECONDS, new RuntimeException("test failure " + i));
        }
    }
}
