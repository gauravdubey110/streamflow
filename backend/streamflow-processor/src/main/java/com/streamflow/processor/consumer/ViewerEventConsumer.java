package com.streamflow.processor.consumer;

import com.streamflow.common.dto.ViewerEventDTO;
import com.streamflow.common.enums.EventType;
import com.streamflow.processor.aggregator.ViewerCountAggregator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer for the {@code viewer-events} topic.
 *
 * <p>SPEC-04 requirements:
 * <ul>
 *   <li>R1 – {@code @KafkaListener} on {@code viewer-events}, group
 *             {@code stream-processor-group}, concurrency = 3 (set on the factory
 *             bean in {@link com.streamflow.processor.config.KafkaConsumerConfig}).</li>
 *   <li>R2 – JOIN → {@code ZADD}; DROP → {@code ZREM}; other types ignored.</li>
 *   <li>R5 – Idempotency is guaranteed by Redis Sorted-Set semantics:
 *             re-delivering the same viewerId ZADD only updates the score.</li>
 *   <li>R6 – Manual ack after successful Redis write. On Redis failure the
 *             exception propagates and the error handler in
 *             {@link com.streamflow.processor.config.KafkaConsumerConfig}
 *             retries up to 3 times then dead-letters to
 *             {@code viewer-events.DLT}.</li>
 * </ul>
 */
@Slf4j
@Component
public class ViewerEventConsumer {

    private final ViewerCountAggregator viewerCountAggregator;

    public ViewerEventConsumer(ViewerCountAggregator viewerCountAggregator) {
        this.viewerCountAggregator = viewerCountAggregator;
    }

    /**
     * Processes a single viewer event from the {@code viewer-events} topic.
     *
     * <p>The container factory is {@code viewerEventListenerContainerFactory}
     * defined in {@link com.streamflow.processor.config.KafkaConsumerConfig},
     * which sets concurrency = 3 and ack mode = MANUAL_IMMEDIATE.
     *
     * @param event           the deserialized {@link ViewerEventDTO}
     * @param acknowledgment  manual acknowledgment handle; called after successful
     *                        Redis write to commit the Kafka offset
     * @throws RuntimeException if the Redis write fails — the error handler
     *                          will retry and eventually dead-letter the record
     */
    @KafkaListener(
            topics = "${streamflow.kafka.topics.viewer-events:viewer-events}",
            groupId = "${spring.kafka.consumer.group-id:stream-processor-group}",
            containerFactory = "viewerEventListenerContainerFactory"
    )
    public void consume(@Payload ViewerEventDTO event, Acknowledgment acknowledgment) {
        try {
            processEvent(event);
            // SPEC-04 R6: ack only after Redis write succeeds
            acknowledgment.acknowledge();
        } catch (Exception e) {
            // Let the exception propagate so the error handler can retry / DLT
            log.error("Failed to process viewer event for stream={} viewer={}: {}",
                    event.streamId(), event.viewerId(), e.getMessage());
            throw e;
        }
    }

    // ── internal ───────────────────────────────────────────────────────────────

    private void processEvent(ViewerEventDTO event) {
        EventType type = event.eventType();

        if (type == EventType.JOIN) {
            viewerCountAggregator.recordJoin(event.streamId(), event.viewerId(), event.timestamp());
            log.trace("Processed JOIN: stream={} viewer={}", event.streamId(), event.viewerId());

        } else if (type == EventType.DROP) {
            viewerCountAggregator.recordDrop(event.streamId(), event.viewerId());
            log.trace("Processed DROP: stream={} viewer={}", event.streamId(), event.viewerId());

        } else {
            // QUALITY_SWITCH, BUFFER_START, BUFFER_END, ERROR — handled in later specs
            log.trace("Skipping event type {} for stream={}", type, event.streamId());
        }
    }
}
