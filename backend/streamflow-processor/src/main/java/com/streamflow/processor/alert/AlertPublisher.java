package com.streamflow.processor.alert;

import com.streamflow.common.constants.KafkaTopics;
import com.streamflow.common.dto.AlertEventDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes {@link AlertEventDTO} records to the {@code alerts} Kafka topic.
 *
 * <p>SPEC-11 R4: the message key is {@code streamId} to guarantee per-stream
 * ordering on the {@code alerts} topic (3 partitions, all keyed by streamId as
 * specified in the Project Plan §6).
 *
 * <p>This class is intentionally thin — no retry logic or batching. The Kafka
 * producer is configured with {@code acks=all} and idempotent delivery; any
 * transient send errors are logged and not propagated so that one failing alert
 * does not abort the entire {@link AlertEngine} scheduling cycle.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AlertPublisher {

    private final KafkaTemplate<String, AlertEventDTO> alertKafkaTemplate;

    /**
     * Sends an alert to the {@code alerts} Kafka topic keyed by {@code streamId}.
     *
     * @param alert the alert event to publish
     */
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
     * Flushes the underlying producer — must be called on engine shutdown to
     * ensure in-flight sends are completed before the JVM exits (SPEC-11 §4).
     */
    public void flush() {
        alertKafkaTemplate.flush();
        log.info("AlertPublisher flushed.");
    }
}
