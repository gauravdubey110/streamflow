package com.streamflow.processor.consumer;

import com.streamflow.common.dto.AlertEventDTO;
import com.streamflow.processor.persistence.CassandraAlertRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer that persists fired alerts to Cassandra.
 *
 * <p>SPEC-17 R5: subscribes to the {@code alerts} topic using a dedicated consumer group
 * ({@code alert-cassandra-group}) so that alert persistence is independent of the
 * API gateway consumer group ({@code api-gateway-group}, SPEC-14).
 *
 * <p>Each received alert is immediately forwarded to
 * {@link CassandraAlertRepository#persist(AlertEventDTO)} which fires an async, bounded
 * write. The Kafka offset is acknowledged after the async dispatch is scheduled (not after
 * the Cassandra write completes) — this keeps the consumer lag near-zero and allows
 * the Cassandra write to complete asynchronously. Failed writes are logged by the repository.
 *
 * <p>The listener uses the default {@code alertListenerContainerFactory} configured in
 * {@link com.streamflow.processor.config.KafkaConsumerConfig} (manual ack, JSON
 * deserialization). A separate container factory is NOT required because alert throughput
 * is orders of magnitude lower than viewer events.
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnBean(CassandraAlertRepository.class)
public class AlertCassandraConsumer {

    private final CassandraAlertRepository cassandraAlertRepository;

    /**
     * Listens to the {@code alerts} topic and writes each alert to Cassandra.
     *
     * @param alert          the deserialized alert event
     * @param acknowledgment manual Kafka acknowledgment
     */
    @KafkaListener(
            topics = "${streamflow.kafka.topics.alerts:alerts}",
            groupId = "alert-cassandra-group",
            containerFactory = "alertListenerContainerFactory"
    )
    public void consume(@Payload AlertEventDTO alert, Acknowledgment acknowledgment) {
        try {
            cassandraAlertRepository.persist(alert);
            acknowledgment.acknowledge();
            log.debug("SPEC-17: alert dispatched to Cassandra: alertId={} stream={} type={}",
                    alert.alertId(), alert.streamId(), alert.alertType());
        } catch (Exception e) {
            log.error("SPEC-17: failed to dispatch alert to Cassandra: alertId={} stream={}: {}",
                    alert.alertId(), alert.streamId(), e.getMessage(), e);
            // Propagate to trigger the error handler / DLT (same pattern as ViewerEventConsumer)
            throw e;
        }
    }
}
