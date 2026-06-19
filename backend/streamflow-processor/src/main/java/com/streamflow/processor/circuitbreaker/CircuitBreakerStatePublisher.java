package com.streamflow.processor.circuitbreaker;

import com.streamflow.common.constants.KafkaTopics;
import com.streamflow.common.dto.CbStateEventDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Bridges the in-process {@link CircuitBreakerStateEvent} Spring event to the
 * {@code cb-events} Kafka topic so that the API gateway (a separate JVM) can
 * consume it and push WebSocket notifications (SPEC-14 R2).
 *
 * <p>The event is published by {@link AlertProcessorCircuitBreaker} on every
 * Resilience4j state transition. This listener converts it to a
 * {@link CbStateEventDTO} and sends it to Kafka keyed by {@code streamId}
 * ({@code "all"} for the global circuit breaker).
 *
 * <p>Implementation note: {@link ApplicationListener} is used instead of
 * {@code @EventListener} because it avoids the need for AOP proxy creation
 * on this component, which keeps the dependency graph simple.
 *
 * <p>Failure policy: if the Kafka send fails (e.g. broker is down), the
 * exception is logged and swallowed. The CB state is already persisted in Redis
 * by {@link AlertProcessorCircuitBreaker}; missing a Kafka push is acceptable
 * because the API gateway will reflect the state on the next REST poll.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class CircuitBreakerStatePublisher implements ApplicationListener<CircuitBreakerStateEvent> {

    /**
     * {@code streamId} used when the circuit breaker is not per-stream.
     *
     * <p>The current implementation uses a single named Resilience4j CB
     * ({@link AlertProcessorCircuitBreaker#CB_NAME}), so all events are
     * published under this sentinel value. The API gateway broadcasts to
     * {@code /topic/streams/all/circuit-breaker}.
     */
    static final String GLOBAL_STREAM_ID = "all";

    private final KafkaTemplate<String, CbStateEventDTO> cbKafkaTemplate;

    /**
     * Receives a CB state-transition Spring event and publishes it to Kafka.
     *
     * @param event the circuit-breaker state-change event
     */
    @Override
    public void onApplicationEvent(CircuitBreakerStateEvent event) {
        CbStateEventDTO dto = new CbStateEventDTO(
                GLOBAL_STREAM_ID,
                event.getPreviousState(),
                event.getCurrentState(),
                event.getReason(),
                event.getOccurredAt()
        );

        log.info("SPEC-14: publishing CB state event to {}: {} → {}",
                KafkaTopics.CB_EVENTS, dto.previousState(), dto.currentState());

        cbKafkaTemplate.send(KafkaTopics.CB_EVENTS, GLOBAL_STREAM_ID, dto)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("SPEC-14: failed to publish CB event to Kafka: {}", ex.getMessage(), ex);
                    } else {
                        log.debug("SPEC-14: CB event published: partition={} offset={}",
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
    }
}
