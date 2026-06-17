package com.streamflow.processor.alert;

import com.streamflow.common.constants.KafkaTopics;
import com.streamflow.common.dto.AlertEventDTO;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes {@link AlertEventDTO} records to the {@code alerts} Kafka topic.
 *
 * <p>SPEC-11 R4: the message key is {@code streamId} to guarantee per-stream
 * ordering on the {@code alerts} topic (3 partitions, all keyed by streamId as
 * specified in the Project Plan §6).
 *
 * <p>SPEC-12 R3: {@link #publish(AlertEventDTO)} is annotated with
 * {@code @CircuitBreaker(name = "alertProcessor", fallbackMethod = "publishFallback")}.
 * When the CB is OPEN (or a Kafka send throws synchronously), the fallback logs a
 * WARN and stores the dropped alert in Redis (SPEC-12 R4).
 *
 * <p>SPEC-12 R4: {@link #publishFallback(AlertEventDTO, Throwable)} stores the
 * dropped alert's ID in the Redis list {@code dropped_alerts:{streamId}}, capped at
 * 100 entries via {@code LTRIM} to prevent unbounded growth.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AlertPublisher {

    /** Maximum number of dropped alert IDs to retain per stream (SPEC-12 R4). */
    public static final int DROPPED_ALERTS_MAX_SIZE = 100;

    /** Redis key prefix for the dropped-alerts list (SPEC-12 R4). */
    public static final String DROPPED_KEY_PREFIX = "dropped_alerts:";

    private final KafkaTemplate<String, AlertEventDTO> alertKafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;

    /**
     * Sends an alert to the {@code alerts} Kafka topic keyed by {@code streamId}.
     *
     * <p>SPEC-12 R3: protected by the {@code alertProcessor} Resilience4j circuit
     * breaker. If the CB is OPEN or a synchronous exception is thrown, the call is
     * routed to {@link #publishFallback(AlertEventDTO, Throwable)}.
     *
     * <p>Note: {@link KafkaTemplate#send} returns a {@code CompletableFuture}; it does
     * NOT throw synchronously on broker failure. To make the CB count failures we rely
     * on the future's exceptional completion: we call {@link java.util.concurrent.CompletableFuture#join()}
     * so that any async Kafka error propagates as a synchronous exception within the
     * CB-decorated method boundary.
     *
     * @param alert the alert event to publish
     */
    @CircuitBreaker(name = "alertProcessor", fallbackMethod = "publishFallback")
    public void publish(AlertEventDTO alert) {
        alertKafkaTemplate.send(KafkaTopics.ALERTS, alert.streamId(), alert)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish alert: alertId={} streamId={} type={}: {}",
                                alert.alertId(), alert.streamId(), alert.alertType(),
                                ex.getMessage(), ex);
                    } else {
                        log.debug("Alert published: alertId={} streamId={} type={} severity={}",
                                alert.alertId(), alert.streamId(),
                                alert.alertType(), alert.severity());
                    }
                });
    }

    /**
     * Fallback invoked by Resilience4j when the {@code alertProcessor} CB is OPEN or
     * when {@link #publish(AlertEventDTO)} throws a synchronous exception.
     *
     * <p>SPEC-12 R4: logs WARN and stores the dropped alert ID in Redis:
     * <pre>
     * LPUSH dropped_alerts:{streamId} {alertId}
     * LTRIM dropped_alerts:{streamId} 0 99
     * </pre>
     *
     * @param alert the alert that was dropped
     * @param t     the exception that caused the fallback (CB open or Kafka error)
     */
    public void publishFallback(AlertEventDTO alert, Throwable t) {
        log.warn("SPEC-12: alert dropped (CB open or Kafka error): alertId={} streamId={} type={} reason={}",
                alert.alertId(), alert.streamId(), alert.alertType(), t.getMessage());

        String droppedKey = DROPPED_KEY_PREFIX + alert.streamId();
        try {
            redisTemplate.opsForList().leftPush(droppedKey, alert.alertId());
            // Cap the list at DROPPED_ALERTS_MAX_SIZE entries (SPEC-12 R4)
            redisTemplate.opsForList().trim(droppedKey, 0, DROPPED_ALERTS_MAX_SIZE - 1);
            log.debug("SPEC-12: stored dropped alert in Redis list key={}", droppedKey);
        } catch (Exception redisEx) {
            log.error("SPEC-12: failed to store dropped alert in Redis: {}", redisEx.getMessage(), redisEx);
        }
    }

    /**
     * Flushes the underlying producer — must be called on engine shutdown to
     * ensure in-flight sends are completed before the JVM exits (SPEC-11 §4).
     */
    public void flush() {
        alertKafkaTemplate.flush();
        log.info("AlertPublisher flushed.");
    }
}
